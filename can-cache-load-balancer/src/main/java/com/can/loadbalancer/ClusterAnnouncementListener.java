package com.can.loadbalancer;

import com.can.loadbalancer.config.LoadBalancerConfig;
import io.quarkus.runtime.Startup;
import io.vertx.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Enumeration;

/**
 * Küme düğümlerinin multicast üzerinden paylaştığı "HELLO" mesajlarını dinleyip
 * yük dengeleyicinin kullanacağı istemci uç noktası görünümünü günceller.
 */
@Startup
@Singleton
public class ClusterAnnouncementListener implements AutoCloseable
{
    private static final Logger LOG = Logger.getLogger(ClusterAnnouncementListener.class);
    private static final int MAX_PACKET_SIZE = 1024;

    private final ClusterMembershipView membershipView;
    private final LoadBalancerConfig config;
    private final Vertx vertx;
    private final boolean enabled;
    private final Map<String, Long> lastSeen = new ConcurrentHashMap<>();

    private MulticastSocket socket;
    private InetAddress groupAddress;
    private NetworkInterface networkInterface;
    private Thread listenerThread;
    private volatile boolean running;
    private long reapTimerId = -1L;

    @Inject
    public ClusterAnnouncementListener(ClusterMembershipView membershipView,
                                       LoadBalancerConfig config,
                                       Vertx vertx)
    {
        this.membershipView = Objects.requireNonNull(membershipView, "membershipView");
        this.config = Objects.requireNonNull(config, "config");
        this.vertx = Objects.requireNonNull(vertx, "vertx");
        this.enabled = config.loadBalancer().enabled();
    }

    @PostConstruct
    void start()
    {
        if (!enabled) {
            LOG.info("Yük dengeleyici devre dışı olduğu için üyelik dinleyicisi başlatılmadı");
            return;
        }

        try {
            int port = config.cluster().discovery().multicastPort();
            socket = new MulticastSocket(port);
            socket.setReuseAddress(true);
            groupAddress = InetAddress.getByName(config.cluster().discovery().multicastGroup());
            networkInterface = selectInterface();
            socket.joinGroup(new InetSocketAddress(groupAddress, port), networkInterface);
        } catch (IOException e) {
            throw new IllegalStateException("Multicast duyurularını dinlemek için soket oluşturulamadı", e);
        }

        running = true;
        listenerThread = new Thread(this::listenLoop, "lb-membership-listener");
        listenerThread.setDaemon(true);
        listenerThread.start();

        long heartbeat = Math.max(1000L, config.cluster().discovery().heartbeatIntervalMillis());
        long reapInterval = Math.max(heartbeat, config.cluster().discovery().failureTimeoutMillis() / 2);
        reapTimerId = vertx.setPeriodic(reapInterval, id -> pruneExpiredMembers());

        LOG.infof("Küme duyuruları %s:%d adresinden dinleniyor", groupAddress.getHostAddress(),
                config.cluster().discovery().multicastPort());
    }

    private void listenLoop()
    {
        byte[] buffer = new byte[MAX_PACKET_SIZE];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        while (running) {
            try {
                socket.receive(packet);
                handlePacket(packet.getData(), packet.getLength());
            } catch (IOException e) {
                if (running) {
                    LOG.debug("Multicast paketini alırken hata oluştu", e);
                }
            }
        }
    }

    private void handlePacket(byte[] data, int length)
    {
        String message = new String(data, 0, length, StandardCharsets.UTF_8);
        String[] parts = message.split("\\|");
        if (parts.length < 6 || !Objects.equals(parts[0], "HELLO")) {
            return;
        }

        String nodeId = parts[1];
        String host = normaliseHost(parts[2]);
        int clientPort;
        try {
            clientPort = Integer.parseInt(parts[5]);
        } catch (NumberFormatException e) {
            clientPort = 0;
        }

        if (clientPort <= 0) {
            return;
        }

        membershipView.upsert(nodeId, host, clientPort);
        lastSeen.put(nodeId, System.currentTimeMillis());
    }

    private String normaliseHost(String host)
    {
        if (host == null || host.isBlank() || Objects.equals(host, "0.0.0.0")) {
            host = config.network().host();
        }
        if (host == null || host.isBlank() || Objects.equals(host, "0.0.0.0")) {
            host = config.cluster().replication().advertiseHost();
        }
        if (host == null || host.isBlank() || Objects.equals(host, "0.0.0.0")) {
            return InetAddress.getLoopbackAddress().getHostAddress();
        }
        return host;
    }

    private void pruneExpiredMembers()
    {
        long now = System.currentTimeMillis();
        long timeout = Math.max(config.cluster().discovery().failureTimeoutMillis(),
                config.cluster().discovery().heartbeatIntervalMillis() * 3);

        lastSeen.entrySet().removeIf(entry -> {
            if (now - entry.getValue() > timeout) {
                membershipView.remove(entry.getKey());
                return true;
            }
            return false;
        });
    }

    private NetworkInterface selectInterface() throws SocketException
    {
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
        throw new SocketException("Uygun multicast arayüzü bulunamadı");
    }

    @PreDestroy
    @Override
    public void close()
    {
        running = false;
        if (reapTimerId >= 0L) {
            vertx.cancelTimer(reapTimerId);
        }
        if (socket != null) {
            try {
                if (groupAddress != null && networkInterface != null) {
                    socket.leaveGroup(new InetSocketAddress(groupAddress, config.cluster().discovery().multicastPort()),
                            networkInterface);
                }
            } catch (IOException ignored) {
            }
            socket.close();
        }
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
        membershipView.clear();
    }
}
