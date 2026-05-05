package com.cancache.agent.cluster.coordination;

import com.cancache.agent.cluster.ClusterState;
import com.cancache.agent.config.AppProperties;
import io.vertx.core.Vertx;
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
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Implements a multicast-based discovery strategy for cluster membership.
 * This strategy uses UDP multicast heartbeats to discover and maintain
 * a list of active cluster nodes.
 */
@Singleton
public class MulticastDiscoveryStrategy implements DiscoveryStrategy {
    private static final Logger LOG = Logger.getLogger(MulticastDiscoveryStrategy.class);
    private static final int MAX_PACKET_SIZE = 1024;

    private final Vertx vertx;
    private final AppProperties.Discovery discoveryConfig;
    private final AppProperties.Replication replicationConfig;
    private final AppProperties.Network networkConfig;
    private final ClusterState clusterState;
    private final ExecutorService taskExecutor; // For processing incoming packets off the listener thread

    private MulticastSocket listenSocket;
    private DatagramSocket sendSocket;
    private InetAddress groupAddress;
    private Thread listenerThread;
    private volatile boolean running;

    private long heartbeatTimerId = -1L;
    private long reapTimerId = -1L;

    // Internal map to track discovered nodes and their last seen timestamp and epoch
    private final Map<String, NodeInfoWithTimestamp> discoveredNodes = new ConcurrentHashMap<>();
    private Consumer<Set<NodeInfo>> membershipListener;

    @Inject
    public MulticastDiscoveryStrategy(Vertx vertx,
                                      AppProperties properties,
                                      ClusterState clusterState) {
        this.vertx = vertx;
        this.discoveryConfig = properties.cluster().discovery();
        this.replicationConfig = properties.cluster().replication();
        this.networkConfig = properties.network();
        this.clusterState = clusterState;

        ThreadFactory threadFactory = Thread.ofVirtual().name("multicast-discovery-task-", 0).factory();
        this.taskExecutor = Executors.newThreadPerTaskExecutor(threadFactory);
    }

    @Override
    public void start(Consumer<Set<NodeInfo>> membershipListener) {
        this.membershipListener = membershipListener;
        try {
            setupSockets();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialise multicast discovery sockets", e);
        }

        running = true;

        listenerThread = new Thread(this::listenLoop, "multicast-listener");
        listenerThread.setDaemon(true);
        listenerThread.start();

        long heartbeat = Math.max(1000L, discoveryConfig.heartbeatIntervalMillis());
        long reapInterval = Math.max(heartbeat, discoveryConfig.failureTimeoutMillis() / 2);

        // Initial announce
        announce();
        heartbeatTimerId = vertx.setPeriodic(heartbeat, _ -> announce());
        reapTimerId = vertx.setPeriodic(reapInterval, _ -> pruneDeadMembers());

        LOG.infof("Multicast discovery started for node %s, listening on %s:%d",
                clusterState.localNodeId(), discoveryConfig.multicastGroup(), discoveryConfig.multicastPort());
    }

    private void setupSockets() throws IOException {
        groupAddress = InetAddress.getByName(discoveryConfig.multicastGroup());
        listenSocket = new MulticastSocket(discoveryConfig.multicastPort());
        listenSocket.setReuseAddress(true);
        NetworkInterface networkInterface = selectInterface();
        tryJoinMulticastGroup(networkInterface);
        sendSocket = new DatagramSocket();
        sendSocket.setReuseAddress(true);
    }

    private void tryJoinMulticastGroup(NetworkInterface selectedInterface) throws IOException {
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
                // Process packet off the listener thread
                taskExecutor.execute(() -> handlePacket(packet.getData(), packet.getLength()));
            } catch (IOException e) {
                if (running) {
                    LOG.warn("Failed to receive multicast discovery packet", e);
                }
            } catch (RejectedExecutionException e) {
                if (running) {
                    LOG.debug("Multicast packet processing task rejected (executor shutting down?)", e);
                }
            }
        }
    }

    private void handlePacket(byte[] data, int length) {
        String message = new String(data, 0, length, StandardCharsets.UTF_8);
        String[] parts = message.split("\\|");
        // Expected format: agreementPackMessage|nodeId|host|port|remoteEpoch|clientPort
        if (parts.length < 5 || !Objects.equals(parts[0], networkConfig.agreementPackMessage())) {
            LOG.debugf("Ignoring malformed or irrelevant multicast packet: %s", message);
            return;
        }

        String nodeId = parts[1];
        if (nodeId.equals(clusterState.localNodeId())) {
            return; // Ignore own heartbeats
        }

        String host = parts[2];
        int port;
        try {
            port = Integer.parseInt(parts[3]);
        } catch (NumberFormatException e) {
            LOG.debugf("Ignoring multicast packet with invalid port: %s", message);
            return;
        }

        long remoteEpoch = 0L;
        try {
            remoteEpoch = Long.parseLong(parts[4]);
        } catch (NumberFormatException ignored) {
            // Epoch might be missing or malformed, default to 0
        }

        NodeInfo newNodeInfo = new NodeInfo(nodeId, host, port);
        NodeInfoWithTimestamp existing = discoveredNodes.get(nodeId);

        if (existing == null || !existing.nodeInfo().equals(newNodeInfo) || remoteEpoch > existing.epoch()) {
            // New node, or node moved (host/port changed), or node has a higher epoch
            discoveredNodes.put(nodeId, new NodeInfoWithTimestamp(newNodeInfo, System.currentTimeMillis(), remoteEpoch));
            LOG.debugf("Discovered/Updated node %s via multicast to %s:%d (epoch: %d)", nodeId, host, port, remoteEpoch);
            notifyMembershipChange();
        } else {
            // Node exists and is the same, just update last seen timestamp
            // Since NodeInfoWithTimestamp is a record (immutable), we replace the entry in the map
            discoveredNodes.put(nodeId, new NodeInfoWithTimestamp(newNodeInfo, System.currentTimeMillis(), existing.epoch()));
            // No need to notify membership change for just a timestamp update, as the set of nodes hasn't changed.
        }
    }

    @Override
    public void announce() {
        String advertisedHost = replicationConfig.advertiseHost();
        int replicationPort = replicationConfig.port();
        String clientAdvertisedHost = resolveClientAdvertisedHost(); // This is from CoordinationService, might need to be passed in or resolved differently
        int clientPort = networkConfig.port(); // This is from CoordinationService, might need to be passed in or resolved differently

        String payload = String.format(networkConfig.agreementPackMessage() + "|%s|%s|%d|%d|%d",
                clusterState.localNodeId(), advertisedHost,
                replicationPort, clusterState.currentEpoch(), clientPort);

        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, groupAddress, discoveryConfig.multicastPort());
        try {
            sendSocket.send(packet);
        } catch (IOException e) {
            LOG.warn("Failed to send multicast heartbeat", e);
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

    private void pruneDeadMembers() {
        long now = System.currentTimeMillis();
        long timeout = Math.max(discoveryConfig.failureTimeoutMillis(),
                discoveryConfig.heartbeatIntervalMillis() * 3);

        boolean changed = false;
        var iterator = discoveredNodes.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, NodeInfoWithTimestamp> entry = iterator.next();
            NodeInfoWithTimestamp node = entry.getValue();
            if (now - node.lastSeen() > timeout) {
                LOG.warnf("Node %s (%s:%d) timed out via multicast discovery", node.nodeInfo().nodeId(), node.nodeInfo().host(), node.nodeInfo().port());
                iterator.remove();
                changed = true;
            }
        }

        if (changed) {
            notifyMembershipChange();
        }
    }

    @Override
    public Set<NodeInfo> getDiscoveredNodes() {
        return discoveredNodes.values().stream()
                .map(NodeInfoWithTimestamp::nodeInfo)
                .collect(Collectors.toUnmodifiableSet());
    }

    private void notifyMembershipChange() {
        if (membershipListener != null) {
            membershipListener.accept(getDiscoveredNodes());
        }
    }

    @Override
    public void close() {
        running = false;
        if (heartbeatTimerId != -1) {
            vertx.cancelTimer(heartbeatTimerId);
        }
        if (reapTimerId != -1) {
            vertx.cancelTimer(reapTimerId);
        }
        taskExecutor.shutdownNow(); // Attempt to shut down the executor

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
            try {
                listenerThread.join(1000); // Wait a bit for the thread to die
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("MulticastDiscoveryStrategy closed.");
    }

    // Helper record to store NodeInfo along with last seen timestamp and epoch
    private record NodeInfoWithTimestamp(NodeInfo nodeInfo, long lastSeen, long epoch) {
        // No need for updateLastSeen method here, as we replace the record in the map.
    }
}
