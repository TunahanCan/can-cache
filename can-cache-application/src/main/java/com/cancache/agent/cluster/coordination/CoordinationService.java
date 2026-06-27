package com.cancache.agent.cluster.coordination;

import com.cancache.agent.cluster.AntiEntropyRepairer;
import com.cancache.agent.cluster.ClusterState;
import com.cancache.agent.cluster.ConsistentHashRing;
import com.cancache.agent.cluster.HintedHandoffService;
import com.cancache.agent.cluster.Node;
import com.cancache.agent.cluster.coordination.SocketConnectionPool.PooledSocket;
import com.cancache.agent.config.AppProperties;
import com.cancache.agent.constants.NodeProtocol;
import com.cancache.agent.core.CacheEngine;
import com.cancache.agent.metric.MetricsRegistry;
import io.vertx.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Multicast tabanlı hafif bir koordinasyon katmanı. Her düğüm belirli aralıklarla
 * kümedeki varlığını duyurur, gelen duyuruları dinleyerek {@link ConsistentHashRing}
 * üzerinde temsil ettiği düğümleri ekler veya çıkarır. Böylece yeni can-cache
 * örnekleri ayağa kalktığında diğer JVM'ler tarafından otomatik olarak keşfedilir
 * ve RAM'deki veriler replikasyon protokolü aracılığıyla senkronize edilir.
 */
@Singleton
public class CoordinationService implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(CoordinationService.class);
    private static final int MAX_PACKET_SIZE = 1024;

    private final ConsistentHashRing<Node<String, String>> ring;
    private final Node<String, String> localNode;
    private final ClusterState clusterState;
    private final HintedHandoffService hintedHandoffService;
    private final CacheEngine<String, String> localEngine;
    private final AppProperties.Discovery discoveryConfig;
    private final AppProperties.Replication replicationConfig;
    private final AppProperties.Network networkConfig;
    private final int replicationFactor;
    private final long hintReplayIntervalMillis;
    private final long antiEntropyIntervalMillis;
    private final int remoteNodePoolSize;
    private final int remoteNodeRequestQueueCapacity;
    private final Vertx vertx;
    private final ExecutorService taskExecutor;
    private final ConnectionPoolManager connectionPoolManager;
    private final String clientAdvertisedHost;
    private final int clientPort;
    private final AntiEntropyRepairer antiEntropyRepairer;
    private final AtomicBoolean antiEntropyInFlight = new AtomicBoolean();

    private final Map<String, RemoteMember> members = new ConcurrentHashMap<>();
    private final Object membershipLock = new Object();

    private MulticastSocket listenSocket;
    private DatagramSocket sendSocket;
    private InetAddress groupAddress;
    private long heartbeatTimerId = -1L;
    private long reapTimerId = -1L;
    private long antiEntropyTimerId = -1L;
    private Thread listenerThread;
    private volatile boolean running;

    @Inject
    public CoordinationService(ConsistentHashRing<Node<String, String>> ring,
                               Node<String, String> localNode,
                               ClusterState clusterState,
                               HintedHandoffService hintedHandoffService,
                               CacheEngine<String, String> localEngine,
                               AppProperties properties,
                               Vertx vertx,
                               MetricsRegistry metrics) {
        this.ring = ring;
        this.localNode = localNode;
        this.clusterState = clusterState;
        this.hintedHandoffService = hintedHandoffService;
        this.localEngine = localEngine;
        var cluster = properties.cluster();
        this.discoveryConfig = cluster.discovery();
        this.replicationConfig = cluster.replication();
        this.networkConfig = properties.network();
        this.replicationFactor = Math.max(1, cluster.replicationFactor());
        var coordination = cluster.coordination();
        this.hintReplayIntervalMillis = Math.max(0L, coordination.hintReplayIntervalMillis());
        this.antiEntropyIntervalMillis = Math.max(0L, coordination.antiEntropyIntervalMillis());
        this.remoteNodePoolSize = coordination.remoteNodePoolSize();
        this.remoteNodeRequestQueueCapacity = coordination.remoteNodeRequestQueueCapacity();
        this.vertx = vertx;
        ThreadFactory threadFactory = Thread.ofVirtual().name("coordination-task-", 0).factory();
        this.taskExecutor = new ThreadPoolExecutor(
                Math.max(1, coordination.taskThreads()),
                Math.max(1, coordination.taskThreads()),
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(Math.max(1, coordination.taskQueueCapacity())),
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy());

        this.connectionPoolManager = new ConnectionPoolManager(
                Runtime.getRuntime().availableProcessors() * 2,
                replicationConfig.connectTimeoutMillis()
        );

        this.clientPort = Math.max(1, networkConfig.port());
        this.clientAdvertisedHost = resolveClientAdvertisedHost();
        this.antiEntropyRepairer = new AntiEntropyRepairer(ring, localNode, localEngine, this.replicationFactor, metrics,
                coordination.antiEntropyMaxRepairsPerRun(), coordination.antiEntropyRepairRatePerSecond());
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

        long heartbeat = Math.max(1000L, discoveryConfig.heartbeatIntervalMillis());
        long reapInterval = Math.max(heartbeat, discoveryConfig.failureTimeoutMillis() / 2);
        broadcastHeartbeat();
        heartbeatTimerId = vertx.setPeriodic(heartbeat, _ -> broadcastHeartbeat());
        reapTimerId = vertx.setPeriodic(reapInterval, _ -> pruneDeadMembers());
        if (antiEntropyIntervalMillis > 0L) {
            antiEntropyTimerId = vertx.setPeriodic(antiEntropyIntervalMillis, _ -> submitAntiEntropyRepair());
        }

        LOG.infof("Coordination service started for node %s, announcing %s:%d", localNode.id(),
                advertisedHost(), replicationConfig.port());
    }

    private void setupSockets() throws IOException
    {
        groupAddress = InetAddress.getByName(discoveryConfig.multicastGroup());
        listenSocket = new MulticastSocket(discoveryConfig.multicastPort());
        listenSocket.setReuseAddress(true);
        NetworkInterface networkInterface = selectInterface();
        tryJoinMulticastGroup(networkInterface);
        sendSocket = new DatagramSocket();
        sendSocket.setReuseAddress(true);
    }

    private void tryJoinMulticastGroup(NetworkInterface selectedInterface) throws IOException
    {
        InetSocketAddress groupSocketAddress =
                new InetSocketAddress(groupAddress, discoveryConfig.multicastPort());

        // First try with the selected interface
        if (selectedInterface != null) {
            try {
                listenSocket.joinGroup(groupSocketAddress, selectedInterface);
                LOG.debugf("Joined multicast group using interface: %s", selectedInterface.getName());
                return;
            } catch (IOException e) {
                LOG.debugf("Failed to join multicast group with selected interface (%s): %s",
                        selectedInterface.getName(), e.getMessage());
            }
        }

        // Try loopback interface as fallback (useful for local development on macOS)
        try {
            NetworkInterface loopback = NetworkInterface.getByInetAddress(InetAddress.getLoopbackAddress());
            if (loopback != null && loopback.supportsMulticast()) {
                listenSocket.joinGroup(groupSocketAddress, loopback);
                LOG.info("Joined multicast group using loopback interface");
                return;
            }
        } catch (IOException e) {
            LOG.debugf("Failed to join multicast group with loopback: %s", e.getMessage());
        }

        // Try all available interfaces
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface ni = interfaces.nextElement();
            try {
                if (ni.isUp() && ni.supportsMulticast()) {
                    listenSocket.joinGroup(groupSocketAddress, ni);
                    LOG.infof("Joined multicast group using interface: %s", ni.getName());
                    return;
                }
            } catch (IOException e) {
                LOG.debugf("Failed to join multicast group with interface %s: %s", ni.getName(), e.getMessage());
            }
        }

        throw new IOException("Could not join multicast group on any network interface");
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
        var loopback = NetworkInterface.getByInetAddress(InetAddress.getLoopbackAddress());
        if (loopback != null) {
            return loopback;
        }
        throw new SocketException("No multicast-capable network interface found");
    }

    private void listenLoop() {
        byte[] buffer = new byte[MAX_PACKET_SIZE];
        while (running) {
            var packet = new DatagramPacket(buffer, buffer.length);
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

    private void handlePacket(byte[] data, int length)
    {
        String message = new String(data, 0, length, StandardCharsets.UTF_8);
        String[] parts = message.split("\\|");
        if (parts.length < 4 || !Objects.equals(parts[0], networkConfig.agreementPackMessage())) {
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

        long remoteEpoch = 0L;
        if (parts.length >= 5) {
            try {
                remoteEpoch = Long.parseLong(parts[4]);
            } catch (NumberFormatException ignored) {
            }
        }

        try {
            final long lambdaRemoteEpoch = remoteEpoch;
            taskExecutor.execute(() -> processMembershipPacket(nodeId, host, port, lambdaRemoteEpoch));
        } catch (RejectedExecutionException e) {
            if (running) {
                LOG.debugf("Coordination task rejected for %s:%d", nodeId, port);
            }
        }
    }

    private void processMembershipPacket(String nodeId, String host, int port, long remoteEpoch) {
        if (!running) {
            return;
        }

        long now = System.currentTimeMillis();
        byte[] idBytes = nodeId.getBytes(StandardCharsets.UTF_8);
        boolean handshakeRequired;
        boolean shouldReplayHints = false;
        RemoteNode replayTarget = null;
        RemoteMember replayMember = null;
        RemoteNode pendingRemoval = null;

        synchronized (membershipLock) {
            RemoteMember existing = members.get(nodeId);
            if (existing == null) {
                handshakeRequired = true;
            } else if (!existing.matches(host, port)) {
                handshakeRequired = true;
                pendingRemoval = existing.node();
                if (pendingRemoval != null) {
                    ring.removeNode(pendingRemoval, existing.idBytes());
                }
                existing.resetBootstrap();
            } else {
                handshakeRequired = false;
                existing.updateLastSeen(now, remoteEpoch);
                clusterState.observeEpoch(remoteEpoch);
                if (existing.shouldReplayHints(now, hintReplayIntervalMillis)) {
                    shouldReplayHints = true;
                    replayTarget = existing.node();
                    replayMember = existing;
                }
            }
        }

        if (!handshakeRequired) {
            if (shouldReplayHints && replayTarget != null) {
                hintedHandoffService.replay(nodeId, replayTarget);
                replayMember.markHintReplayed(System.currentTimeMillis());
            }
            return;
        }

        JoinHandshakeResult join = performJoinHandshake(nodeId, host, port);
        if (join == null || !join.accepted()) {
            return;
        }

        RemoteMember memberForBootstrap = null;
        RemoteNode previousNode = pendingRemoval;
        boolean runBootstrap = false;
        long updateTime = System.currentTimeMillis();

        synchronized (membershipLock) {
            RemoteMember current = members.get(nodeId);
            if (current == null) {
                long previousEpoch = clusterState.currentEpoch();
                runBootstrap = join.epoch() >= previousEpoch;
                clusterState.bumpEpoch();
                clusterState.observeEpoch(join.epoch());

                RemoteNode remoteNode = newRemoteNode(nodeId, host, port);
                RemoteMember newMember = new RemoteMember(remoteNode, idBytes, host, port, updateTime, join.epoch());
                members.put(nodeId, newMember);
                ring.addNode(remoteNode, idBytes);
                LOG.infof("Discovered new cluster member %s at %s:%d", nodeId, host, port);

                memberForBootstrap = newMember;
                shouldReplayHints = true;
                replayTarget = remoteNode;
                replayMember = newMember;
            } else if (!current.matches(host, port)) {
                long previousEpoch = clusterState.currentEpoch();
                runBootstrap = join.epoch() >= previousEpoch;
                clusterState.bumpEpoch();
                clusterState.observeEpoch(join.epoch());

                RemoteNode remoteNode = newRemoteNode(nodeId, host, port);
                previousNode = current.node();
                current.replace(remoteNode, idBytes, host, port, updateTime, join.epoch());
                ring.addNode(remoteNode, idBytes);
                LOG.infof("Cluster member %s moved to %s:%d", nodeId, host, port);

                memberForBootstrap = current;
                shouldReplayHints = true;
                replayTarget = remoteNode;
                replayMember = current;
            } else {
                current.updateLastSeen(updateTime, remoteEpoch);
                clusterState.observeEpoch(remoteEpoch);
                if (current.shouldReplayHints(updateTime, hintReplayIntervalMillis)) {
                    shouldReplayHints = true;
                    replayTarget = current.node();
                    replayMember = current;
                }
            }
        }

        if (memberForBootstrap != null && runBootstrap) {
            bootstrapFrom(memberForBootstrap);
        }

        if (shouldReplayHints && replayTarget != null) {
            hintedHandoffService.replay(nodeId, replayTarget);
            replayMember.markHintReplayed(System.currentTimeMillis());
        }

        if (previousNode != null) {
            closeRemoteNode(previousNode);
        }
    }

    private JoinHandshakeResult performJoinHandshake(String nodeId, String host, int port) {
        PooledSocket pooledSocket = null;
        boolean success = false;
        try {
            pooledSocket = connectionPoolManager.acquire(host, port);
            var out = pooledSocket.out();
            var in = pooledSocket.in();

            byte[] idBytes = clusterState.localNodeIdBytes();
            out.writeByte(NodeProtocol.CMD_JOIN);
            out.writeInt(idBytes.length);
            out.write(idBytes);
            out.writeLong(clusterState.currentEpoch());
            pooledSocket.flush();

            byte response = in.readByte();
            if (response != NodeProtocol.RESP_ACCEPT) {
                LOG.debugf("Join handshake rejected by %s:%d", host, port);
                success = true; // Connection is still valid
                return new JoinHandshakeResult(0L, false);
            }

            int remoteIdLength = in.readInt();
            byte[] remoteIdBytes = in.readNBytes(remoteIdLength);
            if (remoteIdBytes.length != remoteIdLength) {
                throw new EOFException("Incomplete join response payload");
            }
            long remoteEpoch = in.readLong();
            String remoteId = new String(remoteIdBytes, StandardCharsets.UTF_8);
            if (!Objects.equals(remoteId, nodeId)) {
                LOG.warnf("Join handshake id mismatch: expected %s but remote reported %s", nodeId, remoteId);
                success = true;
                return new JoinHandshakeResult(0L, false);
            }
            success = true;
            return new JoinHandshakeResult(remoteEpoch, true);
        } catch (IOException e) {
            LOG.warnf(e, "Failed to perform join handshake with %s:%d", host, port);
            return null;
        } finally {
            if (pooledSocket != null) {
                if (success) {
                    connectionPoolManager.release(host, port, pooledSocket);
                } else {
                    connectionPoolManager.discard(host, port, pooledSocket);
                }
            }
        }
    }

    private void bootstrapFrom(RemoteMember member) {
        if (!member.tryStartBootstrap()) {
            return;
        }

        boolean success = false;
        PooledSocket pooledSocket = null;
        try {
            pooledSocket = connectionPoolManager.acquire(member.host(), member.port());
            var out = pooledSocket.out();
            var in = pooledSocket.in();

            out.writeByte(NodeProtocol.CMD_STREAM);
            pooledSocket.flush();

            long now = System.currentTimeMillis();
            while (true) {
                byte marker;
                try {
                    marker = in.readByte();
                } catch (EOFException eof) {
                    break;
                }
                if (marker == NodeProtocol.STREAM_END_MARKER) {
                    break;
                }
                if (marker != NodeProtocol.STREAM_CHUNK_MARKER) {
                    throw new IOException("Unexpected stream marker: " + marker);
                }

                int keyLen = in.readInt();
                int valueLen = in.readInt();
                long expireAt = in.readLong();

                byte[] keyBytes = in.readNBytes(keyLen);
                byte[] valueBytes = in.readNBytes(valueLen);
                if (keyBytes.length != keyLen || valueBytes.length != valueLen) {
                    throw new EOFException("Incomplete stream payload");
                }

                if (expireAt > 0L && expireAt <= now) continue;

                String key = new String(keyBytes, StandardCharsets.UTF_8);
                if (!replicatesToLocalNode(key)) {
                    continue;
                }
                String value = new String(valueBytes, StandardCharsets.UTF_8);

                Duration ttl = null;
                if (expireAt > 0L) {
                    long ttlMillis = expireAt - now;
                    if (ttlMillis <= 0L) {
                        continue;
                    }
                    ttl = Duration.ofMillis(ttlMillis);
                }
                localNode.set(key, value, ttl);
            }
            success = true;
        } catch (IOException e) {
            LOG.warnf(e, "Failed to synchronise data from %s", member.hostPort());
        } finally {
            if (pooledSocket != null) {
                if (success) {
                    connectionPoolManager.release(member.host(), member.port(), pooledSocket);
                } else {
                    connectionPoolManager.discard(member.host(), member.port(), pooledSocket);
                }
            }
            member.completeBootstrap(success);
        }
    }

    private boolean replicatesToLocalNode(String key)
    {
        List<Node<String, String>> replicas =
                ring.getReplicas(key.getBytes(StandardCharsets.UTF_8), replicationFactor);
        for (Node<String, String> replica : replicas) {
            if (Objects.equals(replica.id(), localNode.id())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Requests the data digest from a remote member for anti-entropy repair.
     * Reserved for future implementation of read-repair and active anti-entropy.
     */
    @SuppressWarnings("unused")
    private long requestDigest(RemoteMember member) throws IOException {
        PooledSocket pooledSocket = null;
        boolean success = false;
        try {
            pooledSocket = connectionPoolManager.acquire(member.host(), member.port());
            var out = pooledSocket.out();
            var in = pooledSocket.in();

            out.writeByte(NodeProtocol.CMD_DIGEST);
            pooledSocket.flush();

            long digest = in.readLong();
            success = true;
            return digest;
        } finally {
            if (pooledSocket != null) {
                if (success) {
                    connectionPoolManager.release(member.host(), member.port(), pooledSocket);
                } else {
                    connectionPoolManager.discard(member.host(), member.port(), pooledSocket);
                }
            }
        }
    }

    /**
     * Computes the expected data digest for keys that should be replicated to a specific node.
     * Reserved for future implementation of active anti-entropy protocol.
     */
    @SuppressWarnings("unused")
    private long computeExpectedDigestFor(String nodeId) {
        final long[] digest = {1125899906842597L};
        localEngine.forEachEntry((key, value, expireAt) -> {
            List<Node<String, String>> replicas =
                    ring.getReplicas(key.getBytes(StandardCharsets.UTF_8), replicationFactor);
            for (Node<String, String> replica : replicas) {
                if (Objects.equals(replica.id(), nodeId)) {
                    long entryHash = 31L * key.hashCode() + Arrays.hashCode(value);
                    entryHash = 31L * entryHash + Long.hashCode(expireAt);
                    digest[0] = 31L * digest[0] + entryHash;
                    break;
                }
            }
        });
        return digest[0];
    }

    void submitAntiEntropyRepair()
    {
        if (!antiEntropyInFlight.compareAndSet(false, true)) {
            return;
        }
        try {
            taskExecutor.execute(() -> {
                try {
                    antiEntropyRepairer.runOnce();
                } finally {
                    antiEntropyInFlight.set(false);
                }
            });
        } catch (RejectedExecutionException e) {
            antiEntropyInFlight.set(false);
            if (running) {
                LOG.debug("Anti-entropy task rejected", e);
            }
        }
    }

    private void broadcastHeartbeat() {
        String payload = String.format(networkConfig.agreementPackMessage() + "|%s|%s|%d|%d|%d",
                localNode.id(), clientAdvertisedHost,
                replicationConfig.port(), clusterState.currentEpoch(), clientPort);

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
        long timeout = Math.max(discoveryConfig.failureTimeoutMillis(),
                discoveryConfig.heartbeatIntervalMillis() * 3);

        synchronized (membershipLock) {
            members.entrySet().removeIf(entry -> {
                RemoteMember member = entry.getValue();
                if (now - member.lastSeen() > timeout) {
                    ring.removeNode(member.node(), member.idBytes());
                    closeRemoteNode(member.node());
                    LOG.warnf("Cluster member %s (%s:%d) timed out", entry.getKey(), member.host(), member.port());
                    clusterState.bumpEpoch();
                    return true;
                }
                return false;
            });
        }
    }

    private String resolveClientAdvertisedHost() {
        String host = networkConfig.host();
        if (host == null || host.isBlank() || Objects.equals(host, "0.0.0.0")) {
            host = replicationConfig.advertiseHost();
        }
        if (host == null || host.isBlank() || Objects.equals(host, "0.0.0.0")) {
            return InetAddress.getLoopbackAddress().getHostAddress();
        }
        return host;
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
    public void close() {
        running = false;
        cancelTimer(heartbeatTimerId);
        cancelTimer(reapTimerId);
        cancelTimer(antiEntropyTimerId);
        taskExecutor.shutdownNow();

        // Connection pool'u kapat
        if (connectionPoolManager != null) {
            connectionPoolManager.close();
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
            members.values().forEach(member -> {
                ring.removeNode(member.node(), member.idBytes());
                closeRemoteNode(member.node());
            });
            members.clear();
        }
    }

    private void closeRemoteNode(RemoteNode node) {
        if (node == null) {
            return;
        }
        try {
            node.close();
        } catch (Exception e) {
            LOG.debugf(e, "Failed to close remote node %s", node.id());
        }
    }

    private RemoteNode newRemoteNode(String nodeId, String host, int port) {
        return new RemoteNode(nodeId, host, port, replicationConfig.connectTimeoutMillis(), vertx,
                remoteNodePoolSize, remoteNodeRequestQueueCapacity);
    }

    private void cancelTimer(long timerId) {
        if (timerId >= 0L) {
            vertx.cancelTimer(timerId);
        }
    }

    private static final class RemoteMember {
        private volatile RemoteNode node;
        private volatile byte[] idBytes;
        private volatile String host;
        private volatile int port;
        private volatile long lastSeen;
        private volatile long epoch;
        private volatile long lastHintReplay;
        private boolean bootstrapped;
        private boolean bootstrapInProgress;

        private RemoteMember(RemoteNode node, byte[] idBytes, String host, int port, long lastSeen, long epoch) {
            this.node = node;
            this.idBytes = idBytes;
            this.host = host;
            this.port = port;
            this.lastSeen = lastSeen;
            this.epoch = epoch;
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

        private String hostPort() {
            return host + ":" + port;
        }

        private long lastSeen() {
            return lastSeen;
        }

        /**
         * Returns the last known epoch of this member.
         * Used for crdt-like conflict resolution during anti-entropy.
         */
        @SuppressWarnings("unused")
        private long epoch() {
            return epoch;
        }

        private void updateLastSeen(long value, long newEpoch) {
            this.lastSeen = value;
            this.epoch = newEpoch;
        }

        private boolean matches(String host, int port) {
            return Objects.equals(this.host, host) && this.port == port;
        }

        private boolean tryStartBootstrap() {
            synchronized (this) {
                if (bootstrapped || bootstrapInProgress) {
                    return false;
                }
                bootstrapInProgress = true;
                return true;
            }
        }

        private void completeBootstrap(boolean success) {
            synchronized (this) {
                bootstrapInProgress = false;
                if (success) {
                    bootstrapped = true;
                }
            }
        }

        private void resetBootstrap() {
            synchronized (this) {
                bootstrapped = false;
                bootstrapInProgress = false;
            }
        }

        private boolean shouldReplayHints(long now, long interval) {
            if (interval <= 0) {
                return true;
            }
            return now - lastHintReplay >= interval;
        }

        private void markHintReplayed(long timestamp) {
            lastHintReplay = timestamp;
        }

        private void replace(RemoteNode node, byte[] idBytes, String host, int port, long lastSeen, long epoch) {
            this.node = node;
            this.idBytes = idBytes;
            this.host = host;
            this.port = port;
            this.lastSeen = lastSeen;
            this.epoch = epoch;
        }
    }
}
