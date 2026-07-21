package com.cancache.agent.cluster.coordination;

import com.cancache.agent.cluster.ClusterState;
import com.cancache.agent.cluster.ConsistentHashRing;
import com.cancache.agent.cluster.HintedHandoffService;
import com.cancache.agent.cluster.Node;
import com.cancache.agent.cluster.coordination.SocketConnectionPool.PooledSocket;
import com.cancache.agent.config.AppProperties;
import com.cancache.agent.constants.NodeProtocol;
import com.cancache.agent.core.CacheEngine;
import io.vertx.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Küme koordinasyon katmanı. {@link DiscoveryStrategy} arayüzü üzerinden
 * düğüm keşfini soyutlar. Gelen keşif duyurularını dinleyerek {@link ConsistentHashRing}
 * üzerinde temsil ettiği düğümleri ekler veya çıkarır. Böylece yeni can-cache
 * örnekleri ayağa kalktığında diğer JVM'ler tarafından otomatik olarak keşfedilir
 * ve RAM'deki veriler replikasyon protokolü aracılığıyla senkronize edilir.
 */

@Singleton
public class CoordinationService implements AutoCloseable
{
    private static final Logger LOG = Logger.getLogger(CoordinationService.class);
    private final ConsistentHashRing<Node<String, String>> ring;
    private final Node<String, String> localNode;
    private final ClusterState clusterState;
    private final HintedHandoffService hintedHandoffService;
    private final CacheEngine<String, String> localEngine;
    private final AppProperties.Replication replicationConfig;
    private final AppProperties.Network networkConfig;
    private final int replicationFactor;
    private final long hintReplayIntervalMillis;
    private final Vertx vertx;
    private final ExecutorService taskExecutor;
    private final ConnectionPoolManager connectionPoolManager;
    private final DiscoveryStrategy discoveryStrategy; // Injected discovery strategy

    private final Map<String, RemoteMember> members = new ConcurrentHashMap<>();
    private final Set<String> memberTasksInProgress = ConcurrentHashMap.newKeySet();
    private final Object membershipLock = new Object();
    private final AtomicReference<Set<NodeInfo>> pendingMembership = new AtomicReference<>();
    private final AtomicReference<Set<NodeInfo>> latestMembership = new AtomicReference<>(Set.of());
    private final AtomicBoolean reconciliationScheduled = new AtomicBoolean();

    private volatile boolean running;
    private volatile long reconciliationTimerId = -1L;

    @Inject
    public CoordinationService(ConsistentHashRing<Node<String, String>> ring,
                               Node<String, String> localNode,
                               ClusterState clusterState,
                               HintedHandoffService hintedHandoffService,
                               CacheEngine<String, String> localEngine,
                               AppProperties properties,
                               Vertx vertx,
                               @Named("selectedDiscoveryStrategy") DiscoveryStrategy discoveryStrategy) { // Inject DiscoveryStrategy
        this.ring = ring;
        this.localNode = localNode;
        this.clusterState = clusterState;
        this.hintedHandoffService = hintedHandoffService;
        this.localEngine = localEngine;
        var cluster = properties.cluster();
        this.replicationConfig = cluster.replication();
        this.networkConfig = properties.network();
        this.replicationFactor = Math.max(1, cluster.replicationFactor());
        var coordination = cluster.coordination();
        this.hintReplayIntervalMillis = Math.max(0L, coordination.hintReplayIntervalMillis());
        this.vertx = vertx;
        ThreadFactory threadFactory = Thread.ofVirtual().name("coordination-task-", 0).factory();
        this.taskExecutor = Executors.newThreadPerTaskExecutor(threadFactory);

        this.connectionPoolManager = new ConnectionPoolManager(
                Runtime.getRuntime().availableProcessors() * 2,
                replicationConfig.connectTimeoutMillis()
        );
        this.discoveryStrategy = discoveryStrategy;
    }

    @PostConstruct
    void start() {
        ring.addNode(localNode, localNode.id().getBytes(StandardCharsets.UTF_8));
        running = true;

        // Discovery callbacks may run on a Vert.x event-loop. Keep all reconciliation,
        // handshakes and hinted-handoff replay on virtual worker threads.
        discoveryStrategy.start(this::scheduleMembershipChange);

        // Announce local node presence (if strategy supports it)
        discoveryStrategy.announce();

        long reconciliationInterval = hintReplayIntervalMillis > 0L
                ? hintReplayIntervalMillis
                : 1_000L;
        reconciliationTimerId = vertx.setPeriodic(reconciliationInterval,
                ignored -> scheduleDiscoverySnapshot());

        LOG.infof("Coordination service started for node %s. Discovery strategy: %s",
                localNode.id(), discoveryStrategy.getClass().getSimpleName());
    }

    /**
     * Captures the latest membership snapshot and coalesces callbacks. Discovery
     * implementations are allowed to invoke their listener from an event-loop, so
     * no reconciliation work is performed by the caller.
     */
    private void scheduleMembershipChange(Set<NodeInfo> discoveredNodes) {
        if (!running) {
            return;
        }
        Set<NodeInfo> snapshot = Set.copyOf(discoveredNodes);
        latestMembership.set(snapshot);
        pendingMembership.set(snapshot);
        scheduleReconciliationWorker();
    }

    private void scheduleDiscoverySnapshot() {
        if (!running) {
            return;
        }
        executeTask(() -> {
            try {
                scheduleMembershipChange(discoveryStrategy.getDiscoveredNodes());
            } catch (RuntimeException e) {
                if (running) {
                    LOG.warn("Failed to obtain the current discovery snapshot", e);
                }
            }
        }, "discovery snapshot");
    }

    private void scheduleReconciliationWorker() {
        if (!running || !reconciliationScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            taskExecutor.execute(this::drainMembershipChanges);
        } catch (RejectedExecutionException e) {
            reconciliationScheduled.set(false);
            if (running) {
                LOG.debug("Coordination reconciliation rejected (executor shutting down?)", e);
            }
        }
    }

    private void drainMembershipChanges() {
        try {
            while (running) {
                Set<NodeInfo> discoveredNodes = pendingMembership.getAndSet(null);
                if (discoveredNodes == null) {
                    return;
                }
                handleMembershipChange(discoveredNodes);
            }
        } finally {
            reconciliationScheduled.set(false);
            if (running && pendingMembership.get() != null) {
                scheduleReconciliationWorker();
            }
        }
    }

    /**
     * Reconciles a discovery snapshot from a coordination worker thread.
     */
    private void handleMembershipChange(Set<NodeInfo> newDiscoveredNodes) {
        if (!running) {
            return;
        }

        Set<String> newDiscoveredNodeIds = newDiscoveredNodes.stream()
                .map(NodeInfo::nodeId)
                .collect(Collectors.toSet());

        List<RemoteMember> removedMembers = new ArrayList<>();
        synchronized (membershipLock) {
            // 1. Remove nodes that are no longer discovered
            Set<String> membersToRemove = new HashSet<>();
            for (String existingNodeId : members.keySet()) {
                if (!newDiscoveredNodeIds.contains(existingNodeId) && !existingNodeId.equals(localNode.id())) {
                    membersToRemove.add(existingNodeId);
                }
            }
            for (String nodeId : membersToRemove) {
                RemoteMember member = members.remove(nodeId);
                if (member != null) {
                    ring.removeNode(member.node(), member.idBytes());
                    removedMembers.add(member);
                    clusterState.bumpEpoch(); // Membership change, bump epoch
                }
            }
        }

        // RemoteNode.close waits for the NetClient to close, so it must never run
        // while membershipLock is held.
        for (RemoteMember member : removedMembers) {
            closeRemoteNode(member.node());
            LOG.warnf("Cluster member %s (%s:%d) removed from membership (no longer discovered)",
                    member.node().id(), member.host(), member.port());
        }

        // 2. Add or update newly discovered nodes
        for (NodeInfo nodeInfo : newDiscoveredNodes) {
            if (nodeInfo.nodeId().equals(localNode.id())) {
                continue; // Skip local node
            }
            scheduleDiscoveredNode(nodeInfo);
        }
    }

    private void scheduleDiscoveredNode(NodeInfo nodeInfo) {
        if (!running || !memberTasksInProgress.add(nodeInfo.nodeId())) {
            return;
        }
        try {
            taskExecutor.execute(() -> {
                try {
                    processDiscoveredNode(nodeInfo);
                } finally {
                    memberTasksInProgress.remove(nodeInfo.nodeId());
                    if (running) {
                        latestMembership.get().stream()
                                .filter(latest -> latest.nodeId().equals(nodeInfo.nodeId()))
                                .filter(latest -> !latest.equals(nodeInfo))
                                .findFirst()
                                .ifPresent(this::scheduleDiscoveredNode);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            memberTasksInProgress.remove(nodeInfo.nodeId());
            if (running) {
                LOG.debugf(e, "Coordination task rejected for member %s (executor shutting down?)",
                        nodeInfo.nodeId());
            }
        }
    }

    private void executeTask(Runnable task, String description) {
        try {
            taskExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            if (running) {
                LOG.debugf(e, "Coordination task rejected for %s (executor shutting down?)", description);
            }
        }
    }

    /**
     * Processes a single discovered node, performing handshake and bootstrap if necessary.
     * This method is executed in a worker thread.
     */
    private void processDiscoveredNode(NodeInfo nodeInfo) {
        if (!running || !latestMembership.get().contains(nodeInfo)) {
            return;
        }

        long now = System.currentTimeMillis();
        byte[] idBytes = nodeInfo.nodeId().getBytes(StandardCharsets.UTF_8);
        boolean handshakeRequired;
        boolean shouldReplayHints = false;
        RemoteNode replayTarget = null;
        RemoteMember replayMember = null;

        synchronized (membershipLock)
        {
            RemoteMember existing = members.get(nodeInfo.nodeId());
            if (existing == null) {
                handshakeRequired = true;
            } else if (!existing.matches(nodeInfo)) { // Node moved (host/port changed)
                handshakeRequired = true;
            } else {
                // Node exists and matches, just update last seen and check for hints
                handshakeRequired = false;
                // Note: remoteEpoch is not directly available from NodeInfo in this generic callback,
                // it would be part of the handshake or specific discovery strategy's internal state.
                // For now, we'll use the epoch from the existing member or assume it's updated via handshake.
                // The actual remote epoch will be obtained during the handshake.
                existing.updateLastSeen(now, clusterState.currentEpoch()); // Use local epoch for now, actual remote epoch comes from handshake
                clusterState.observeEpoch(existing.epoch()); // Observe the epoch of the existing member

                if (existing.shouldReplayHints(now, hintReplayIntervalMillis)) {
                    shouldReplayHints = true;
                    replayTarget = existing.node();
                    replayMember = existing;
                }
            }
        }

        if (!handshakeRequired) {
            if (shouldReplayHints && replayTarget != null) {
                hintedHandoffService.replay(nodeInfo.nodeId(), replayTarget);
                replayMember.markHintReplayed(System.currentTimeMillis());
            }
            return;
        }

        // Perform handshake for new or moved nodes
        JoinHandshakeResult join = performJoinHandshake(nodeInfo);
        if (join == null || !join.accepted()) {
            LOG.warnf("Join handshake failed or rejected for %s:%d. Not adding to cluster.", nodeInfo.host(), nodeInfo.port());
            return;
        }

        if (!running || !latestMembership.get().contains(nodeInfo)) {
            LOG.debugf("Discarding stale join result for %s at %s:%d",
                    nodeInfo.nodeId(), nodeInfo.host(), nodeInfo.port());
            return;
        }

        RemoteMember memberForBootstrap = null;
        RemoteNode previousNode = null;
        boolean runBootstrap = false;
        long updateTime = System.currentTimeMillis();

        synchronized (membershipLock)
        {
            if (!running) {
                return;
            }
            RemoteMember current = members.get(nodeInfo.nodeId());
            if (current == null) {
                // Truly a new member after handshake
                long previousEpoch = clusterState.currentEpoch();
                runBootstrap = join.epoch() >= previousEpoch; // Bootstrap if remote epoch is newer or equal
                clusterState.bumpEpoch(); // Local membership changed, bump epoch
                clusterState.observeEpoch(join.epoch()); // Observe the epoch from handshake

                RemoteNode remoteNode = new RemoteNode(nodeInfo.nodeId(), nodeInfo.host(), nodeInfo.port(), replicationConfig.connectTimeoutMillis(), vertx);
                RemoteMember newMember = new RemoteMember(remoteNode, idBytes, nodeInfo, updateTime, join.epoch());
                members.put(nodeInfo.nodeId(), newMember);
                ring.addNode(remoteNode, idBytes);
                LOG.infof("Discovered new cluster member %s at %s:%d (epoch: %d)", nodeInfo.nodeId(), nodeInfo.host(), nodeInfo.port(), join.epoch());

                memberForBootstrap = newMember;
                shouldReplayHints = true;
                replayTarget = remoteNode;
                replayMember = newMember;
            } else if (!current.matches(nodeInfo)) {
                // Existing member but moved (host/port changed) - should have been handled by pendingRemoval, but double check
                LOG.warnf("Member %s changed address from %s:%d to %s:%d after handshake. Updating.",
                        nodeInfo.nodeId(), current.host(), current.port(), nodeInfo.host(), nodeInfo.port());

                long previousEpoch = clusterState.currentEpoch();
                runBootstrap = join.epoch() >= previousEpoch;
                clusterState.bumpEpoch();
                clusterState.observeEpoch(join.epoch());

                RemoteNode remoteNode = new RemoteNode(nodeInfo.nodeId(), nodeInfo.host(), nodeInfo.port(), replicationConfig.connectTimeoutMillis(), vertx);
                previousNode = current.node(); // Store old node for closing
                current.replace(remoteNode, idBytes, nodeInfo, updateTime, join.epoch());
                // addNode replaces virtual nodes with the same id atomically under
                // the ring lock. The old member remains routable until this point.
                ring.addNode(remoteNode, idBytes);
                LOG.infof("Cluster member %s moved to %s:%d (epoch: %d)", nodeInfo.nodeId(), nodeInfo.host(), nodeInfo.port(), join.epoch());

                memberForBootstrap = current;
                shouldReplayHints = true;
                replayTarget = remoteNode;
                replayMember = current;
            } else {
                // Member already known and matches, just update last seen and epoch from handshake
                current.updateLastSeen(updateTime, join.epoch());
                clusterState.observeEpoch(join.epoch());
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
            hintedHandoffService.replay(nodeInfo.nodeId(), replayTarget);
            replayMember.markHintReplayed(System.currentTimeMillis());
        }

        if (previousNode != null) {
            closeRemoteNode(previousNode);
        }
    }

    private JoinHandshakeResult performJoinHandshake(NodeInfo nodeInfo) {
        PooledSocket pooledSocket = null;
        boolean success = false;
        try {
            pooledSocket = connectionPoolManager.acquire(nodeInfo.host(), nodeInfo.port());
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
                LOG.debugf("Join handshake rejected by %s:%d", nodeInfo.host(), nodeInfo.port());
                success = true; // Connection is still valid, just rejected
                return new JoinHandshakeResult(0L, false);
            }

            int remoteIdLength = in.readInt();
            byte[] remoteIdBytes = in.readNBytes(remoteIdLength);
            if (remoteIdBytes.length != remoteIdLength) {
                throw new EOFException("Incomplete join response payload");
            }
            long remoteEpoch = in.readLong();
            String remoteId = new String(remoteIdBytes, StandardCharsets.UTF_8);
            if (!Objects.equals(remoteId, nodeInfo.nodeId())) {
                LOG.warnf("Join handshake id mismatch: expected %s but remote reported %s from %s:%d",
                        nodeInfo.nodeId(), remoteId, nodeInfo.host(), nodeInfo.port());
                success = true; // Connection is still valid, but mismatch
                return new JoinHandshakeResult(0L, false);
            }
            success = true;
            return new JoinHandshakeResult(remoteEpoch, true);
        } catch (IOException e) {
            LOG.warnf(e, "Failed to perform join handshake with %s:%d", nodeInfo.host(), nodeInfo.port());
            return null;
        } finally {
            if (pooledSocket != null) {
                if (success) {
                    connectionPoolManager.release(nodeInfo.host(), nodeInfo.port(), pooledSocket);
                } else {
                    connectionPoolManager.discard(nodeInfo.host(), nodeInfo.port(), pooledSocket);
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

    @PreDestroy
    @Override
    public void close() {
        running = false;
        pendingMembership.set(null);
        latestMembership.set(Set.of());
        memberTasksInProgress.clear();
        long timerId = reconciliationTimerId;
        reconciliationTimerId = -1L;
        if (timerId >= 0L) {
            vertx.cancelTimer(timerId);
        }
        taskExecutor.shutdownNow();

        // Connection pool'u kapat
        if (connectionPoolManager != null) {
            connectionPoolManager.close();
        }

        List<RemoteMember> membersToClose;
        synchronized (membershipLock) {
            membersToClose = new ArrayList<>(members.values());
            membersToClose.forEach(member ->
                    ring.removeNode(member.node(), member.idBytes()));
            members.clear();
        }
        Runnable closeMembers = () -> {
            membersToClose.forEach(member -> closeRemoteNode(member.node()));
            LOG.info("CoordinationService closed.");
        };
        var context = Vertx.currentContext();
        if (context != null && context.isEventLoopContext()) {
            Thread.startVirtualThread(closeMembers);
        } else {
            closeMembers.run();
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

    private static final class RemoteMember {
        private volatile RemoteNode node;
        private volatile byte[] idBytes;
        private volatile NodeInfo nodeInfo; // Store NodeInfo directly
        private volatile long lastSeen;
        private volatile long epoch;
        private volatile long lastHintReplay;
        private boolean bootstrapped;
        private boolean bootstrapInProgress;

        private RemoteMember(RemoteNode node, byte[] idBytes, NodeInfo nodeInfo, long lastSeen, long epoch) {
            this.node = node;
            this.idBytes = idBytes;
            this.nodeInfo = nodeInfo; // Use NodeInfo directly
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
            return nodeInfo.host();
        }

        private int port() {
            return nodeInfo.port();
        }

        private String hostPort() {
            return nodeInfo.host() + ":" + nodeInfo.port();
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

        private boolean matches(NodeInfo otherNodeInfo) {
            return this.nodeInfo.equals(otherNodeInfo);
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

        private boolean shouldReplayHints(long now, long interval) {
            if (interval <= 0) {
                return true;
            }
            return now - lastHintReplay >= interval;
        }

        private void markHintReplayed(long timestamp) {
            lastHintReplay = timestamp;
        }

        private void replace(RemoteNode node, byte[] idBytes, NodeInfo nodeInfo, long lastSeen, long epoch) {
            this.node = node;
            this.idBytes = idBytes;
            this.nodeInfo = nodeInfo; // Update NodeInfo
            this.lastSeen = lastSeen;
            this.epoch = epoch;
        }
    }
}
