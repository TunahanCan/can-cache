package com.can.cluster.coordination;

import com.can.cluster.ConsistentHashRing;
import com.can.cluster.Node;
import com.can.config.AppProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Multicast tabanlı hafif bir koordinasyon katmanı. Her düğüm belirli aralıklarla
 * kümedeki varlığını duyurur, gelen duyuruları dinleyerek {@link ConsistentHashRing}
 * üzerinde temsil ettiği düğümleri ekler veya çıkarır. Böylece yeni can-cache
 * örnekleri ayağa kalktığında diğer JVM'ler tarafından otomatik olarak keşfedilir
 * ve RAM'deki veriler replikasyon protokolü aracılığıyla senkronize edilir.
 */
@Singleton
public class CoordinationService implements AutoCloseable
{
    private static final Logger LOG = Logger.getLogger(CoordinationService.class);
    private static final int MAX_PACKET_SIZE = 1024;

    private final ConsistentHashRing<Node<String, String>> ring;
    private final Node<String, String> localNode;
    private final AppProperties.Discovery discoveryConfig;
    private final AppProperties.Replication replicationConfig;

    private final Map<String, RemoteMember> members = new ConcurrentHashMap<>();
    private final Object membershipLock = new Object();

    private MulticastSocket listenSocket;
    private DatagramSocket sendSocket;
    private InetAddress groupAddress;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> heartbeatTask;
    private ScheduledFuture<?> reapTask;
    private Thread listenerThread;
    private volatile boolean running;

    @Inject
    public CoordinationService(ConsistentHashRing<Node<String, String>> ring,
                               Node<String, String> localNode,
                               AppProperties properties) {
        this.ring = ring;
        this.localNode = localNode;
        var cluster = properties.cluster();
        this.discoveryConfig = cluster.discovery();
        this.replicationConfig = cluster.replication();
    }

    @PostConstruct
    void start() {
        try {
            setupSockets();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialise coordination sockets", e);
        }

        ring.addNode(localNode, localNode.id().getBytes(StandardCharsets.UTF_8));
        running = true;

        listenerThread = new Thread(this::listenLoop, "coordination-listener");
        listenerThread.setDaemon(true);
        listenerThread.start();

        scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
        long heartbeat = Math.max(1000L, discoveryConfig.heartbeatIntervalMillis());
        long reapInterval = Math.max(heartbeat, discoveryConfig.failureTimeoutMillis() / 2);
        heartbeatTask = scheduler.scheduleAtFixedRate(this::broadcastHeartbeat, 0L, heartbeat, TimeUnit.MILLISECONDS);
        reapTask = scheduler.scheduleAtFixedRate(this::pruneDeadMembers, reapInterval, reapInterval, TimeUnit.MILLISECONDS);

        LOG.infof("Coordination service started for node %s, announcing %s:%d", localNode.id(),
                advertisedHost(), replicationConfig.port());
    }

    private void setupSockets() throws IOException {
        groupAddress = InetAddress.getByName(discoveryConfig.multicastGroup());
        listenSocket = new MulticastSocket(discoveryConfig.multicastPort());
        listenSocket.setReuseAddress(true);
        NetworkInterface networkInterface = selectInterface();
        listenSocket.joinGroup(new InetSocketAddress(groupAddress, discoveryConfig.multicastPort()), networkInterface);
        sendSocket = new DatagramSocket();
        sendSocket.setReuseAddress(true);
    }

    private NetworkInterface selectInterface() throws SocketException {
        // Önce bind host'u deneyelim, değilse multicast destekleyen ilk arayüzü seçelim.
        try {
            InetAddress bindAddress = InetAddress.getByName(replicationConfig.bindHost());
            NetworkInterface ni = NetworkInterface.getByInetAddress(bindAddress);
            if (ni != null && ni.isUp() && ni.supportsMulticast()) {
                return ni;
            }
        } catch (IOException ignored) {
        }

        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface ni = interfaces.nextElement();
            if (ni.isUp() && ni.supportsMulticast() && !ni.isLoopback()) {
                return ni;
            }
        }
        NetworkInterface loopback = NetworkInterface.getByInetAddress(InetAddress.getLoopbackAddress());
        if (loopback != null) {
            return loopback;
        }
        throw new SocketException("No multicast-capable network interface found");
    }

    private void listenLoop() {
        byte[] buffer = new byte[MAX_PACKET_SIZE];
        while (running) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                listenSocket.receive(packet);
                handlePacket(packet.getData(), packet.getLength());
            } catch (IOException e) {
                if (running) {
                    LOG.warn("Failed to receive coordination packet", e);
                }
            }
        }
    }

    private void handlePacket(byte[] data, int length) {
        String message = new String(data, 0, length, StandardCharsets.UTF_8);
        String[] parts = message.split("\\|");
        if (parts.length != 4 || !Objects.equals(parts[0], "HELLO")) {
            return;
        }

        String nodeId = parts[1];
        if (nodeId.equals(localNode.id())) {
            return;
        }

        String host = parts[2];
        int port;
        try {
            port = Integer.parseInt(parts[3]);
        } catch (NumberFormatException e) {
            LOG.debugf("Ignoring coordination packet with invalid port: %s", message);
            return;
        }

        long now = System.currentTimeMillis();
        byte[] idBytes = nodeId.getBytes(StandardCharsets.UTF_8);

        synchronized (membershipLock) {
            RemoteMember existing = members.get(nodeId);
            if (existing == null) {
                RemoteNode remoteNode = new RemoteNode(nodeId, host, port, replicationConfig.connectTimeoutMillis());
                members.put(nodeId, new RemoteMember(remoteNode, idBytes, host, port, now));
                ring.addNode(remoteNode, idBytes);
                LOG.infof("Discovered new cluster member %s at %s:%d", nodeId, host, port);
            } else if (!existing.matches(host, port)) {
                ring.removeNode(existing.node(), existing.idBytes());
                RemoteNode remoteNode = new RemoteNode(nodeId, host, port, replicationConfig.connectTimeoutMillis());
                members.put(nodeId, new RemoteMember(remoteNode, idBytes, host, port, now));
                ring.addNode(remoteNode, idBytes);
                LOG.infof("Cluster member %s moved to %s:%d", nodeId, host, port);
            } else {
                existing.updateLastSeen(now);
            }
        }
    }

    private void broadcastHeartbeat() {
        String payload = String.format("HELLO|%s|%s|%d", localNode.id(), advertisedHost(), replicationConfig.port());
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, groupAddress, discoveryConfig.multicastPort());
        try {
            sendSocket.send(packet);
        } catch (IOException e) {
            LOG.warn("Failed to send coordination heartbeat", e);
        }
    }

    private void pruneDeadMembers() {
        long now = System.currentTimeMillis();
        long timeout = Math.max(discoveryConfig.failureTimeoutMillis(), discoveryConfig.heartbeatIntervalMillis() * 3);

        synchronized (membershipLock) {
            members.entrySet().removeIf(entry -> {
                RemoteMember member = entry.getValue();
                if (now - member.lastSeen() > timeout) {
                    ring.removeNode(member.node(), member.idBytes());
                    LOG.warnf("Cluster member %s (%s:%d) timed out", entry.getKey(), member.host(), member.port());
                    return true;
                }
                return false;
            });
        }
    }

    private String advertisedHost() {
        String host = replicationConfig.advertiseHost();
        if (host == null || host.isBlank() || Objects.equals(host, "0.0.0.0")) {
            return InetAddress.getLoopbackAddress().getHostAddress();
        }
        return host;
    }

    @PreDestroy
    @Override
    public void close()
    {
        running = false;
        if (heartbeatTask != null) {
            heartbeatTask.cancel(true);
        }
        if (reapTask != null) {
            reapTask.cancel(true);
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (listenSocket != null) {
            try {
                listenSocket.close();
            } catch (Exception ignored) {
            }
        }
        if (sendSocket != null) {
            sendSocket.close();
        }
        if (listenerThread != null) {
            listenerThread.interrupt();
        }

        synchronized (membershipLock) {
            members.values().forEach(member -> ring.removeNode(member.node(), member.idBytes()));
            members.clear();
        }
    }

    private static final class RemoteMember
    {
        private final RemoteNode node;
        private final byte[] idBytes;
        private final String host;
        private final int port;
        private volatile long lastSeen;

        private RemoteMember(RemoteNode node, byte[] idBytes, String host, int port, long lastSeen) {
            this.node = node;
            this.idBytes = idBytes;
            this.host = host;
            this.port = port;
            this.lastSeen = lastSeen;
        }

        private RemoteNode node() {
            return node;
        }

        private byte[] idBytes() {
            return idBytes;
        }

        private String host() {
            return host;
        }

        private int port() {
            return port;
        }

        private long lastSeen() {
            return lastSeen;
        }

        private void updateLastSeen(long value) {
            this.lastSeen = value;
        }

        private boolean matches(String host, int port) {
            return Objects.equals(this.host, host) && this.port == port;
        }
    }
}
