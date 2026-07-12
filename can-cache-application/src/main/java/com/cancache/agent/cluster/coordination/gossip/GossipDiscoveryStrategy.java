package com.cancache.agent.cluster.coordination.gossip;

import com.cancache.agent.cluster.ClusterState;
import com.cancache.agent.cluster.coordination.DiscoveryStrategy;
import com.cancache.agent.cluster.coordination.NodeInfo;
import com.cancache.agent.config.AppProperties;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import io.vertx.core.net.SocketAddress;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Implements a Gossip-based discovery strategy for cluster membership, inspired by the SWIM protocol.
 * This strategy is decentralized and resilient to network partitions,
 * suitable for cloud environments where multicast is often restricted.
 *
 * This is a simplified skeleton implementation focusing on core gossip mechanisms (ping, ack, gossip exchange, failure detection).
 * A full production-ready SWIM implementation would include more sophisticated failure detection (e.g., using phi accrual),
 * more optimized gossip exchange (e.g., sending deltas or random subsets of members), and handling of network partitions.
 */
@Singleton
public class GossipDiscoveryStrategy implements DiscoveryStrategy
{

    private static final Logger LOG = Logger.getLogger(GossipDiscoveryStrategy.class);
    private final Vertx vertx;
    private final AppProperties.Gossip gossipConfig;
    private final ClusterState clusterState;
    private final NodeInfo localNodeInfo;

    private DatagramSocket socket;
    private Consumer<Set<NodeInfo>> membershipListener;

    // Membership list: Map of nodeId to GossipMember
    // This map holds the local view of the cluster membership.
    private final Map<String, GossipMember> members = new ConcurrentHashMap<>();
    private volatile Set<NodeInfo> lastKnownActiveNodes = Collections.emptySet(); // To optimize membership change notifications

    // Timers for periodic tasks
    private long pingTimerId = -1;
    private long gossipTimerId = -1;
    private long cleanupTimerId = -1;

    public GossipDiscoveryStrategy(Vertx vertx, AppProperties properties, ClusterState clusterState)
    {
        this.vertx = vertx;
        this.gossipConfig = properties.cluster().gossip();
        this.clusterState = clusterState;

        // Determine local node info for gossip communication
        String advertisedHost = properties.cluster().replication().advertiseHost();
        int replicationPort = properties.cluster().replication().port();
        this.localNodeInfo = new NodeInfo(clusterState.localNodeId(), advertisedHost, replicationPort);
    }

    @Override
    public void start(Consumer<Set<NodeInfo>> membershipListener)
     {
        this.membershipListener = membershipListener;
        // Add local node to membership list with ALIVE status
        members.put(localNodeInfo.nodeId(), new GossipMember(localNodeInfo, clusterState.currentEpoch(), System.currentTimeMillis(), MemberStatus.ALIVE));
        // Setup UDP socket for gossip communication
        DatagramSocketOptions options = new DatagramSocketOptions()
                .setSendBufferSize(GossipMessageCodec.MAX_PACKET_BYTES)
                .setReceiveBufferSize(GossipMessageCodec.MAX_PACKET_BYTES)
                .setReuseAddress(true);

        socket = vertx.createDatagramSocket(options);
        socket.listen(gossipConfig.port(), gossipConfig.bindHost())
                .onSuccess(s -> {
                    LOG.infof("Gossip socket listening on %s:%d", gossipConfig.bindHost(), gossipConfig.port());
                    s.handler(packet -> handleGossipPacket(packet.data(), packet.sender()));
                    startGossipTimers();
                    seedMembership(); // Initial seeding from configured seed nodes
                })
                .onFailure(e -> LOG.errorf(e, "Failed to bind gossip socket on %s:%d", gossipConfig.bindHost(), gossipConfig.port()));
    }

    /**
     * Initializes the membership list with configured seed nodes.
     * Seed nodes are crucial for a new node to join an existing cluster.
     */
    private void seedMembership() {
        for (String seed : gossipConfig.seedNodes()) {
            try {
                String[] parts = seed.split(":");
                if (parts.length == 2) {
                    String host = parts[0];
                    int port = Integer.parseInt(parts[1]);
                    // For seed nodes, if a specific nodeId isn't provided, we derive one from host:port.
                    // In a real system, seed nodes would ideally have well-known IDs.
                    String seedNodeId = "seed-" + host + ":" + port; // Use a distinct prefix for derived IDs
                    NodeInfo seedNodeInfo = new NodeInfo(seedNodeId, host, port);

                    // Only add if it's not the local node and not already present in the membership list
                    if (!seedNodeInfo.equals(localNodeInfo) && !members.containsKey(seedNodeId)) {
                        members.put(seedNodeId, new GossipMember(seedNodeInfo, 0, System.currentTimeMillis(), MemberStatus.ALIVE));
                        LOG.debugf("Added seed node %s to membership list", seedNodeInfo);
                    }
                } else {
                    LOG.warnf("Invalid seed node format: %s. Expected host:port", seed);
                }
            } catch (NumberFormatException e) {
                LOG.warnf("Invalid port in seed node: %s", seed);
            }
        }
        notifyMembershipChange(); // Notify after initial seeding, as the active node set might have changed
    }

    /**
     * Starts periodic timers for pinging, gossip exchange, and membership cleanup.
     */
    private void startGossipTimers() {
        // Periodic pinging of a random member to detect failures
        pingTimerId = vertx.setPeriodic(gossipConfig.pingInterval().toMillis(), id -> pingRandomMember());

        // Periodic gossip exchange to propagate membership information
        gossipTimerId = vertx.setPeriodic(gossipConfig.gossipInterval().toMillis(), id -> exchangeGossip());

        // Periodic cleanup of dead members and marking suspects
        cleanupTimerId = vertx.setPeriodic(gossipConfig.cleanupInterval().toMillis(), id -> cleanupDeadMembers());
    }

    /**
     * Handles incoming UDP gossip packets.
     * Deserializes the message and dispatches it to the appropriate processing method.
     */
    private void handleGossipPacket(Buffer buffer, SocketAddress sender) {
        if (buffer.length() > GossipMessageCodec.MAX_PACKET_BYTES) {
            LOG.warnf("Ignoring oversized gossip message from %s (%d bytes)", sender, buffer.length());
            return;
        }
        try {
            GossipMessage message = GossipMessageCodec.decode(buffer.getBytes());
            LOG.debugf("Received gossip message %s from %s (sender: %s)", message.getType(), sender, message.getSenderId());

            // Process message based on type
            switch (message.getType()) {
                case GOSSIP:
                    processGossip(message);
                    break;
                case PING:
                    processPing(message, sender);
                    break;
                case ACK:
                    processAck(message);
                    break;
                case PING_REQ:
                    processPingReq(message, sender);
                    break;
            }
            refreshKnownSender(message.getSenderId());
            // notifyMembershipChange() is called within processGossip/Ack/cleanupDeadMembers if actual changes occur
        } catch (IOException e) {
            LOG.warnf(e, "Failed to decode gossip message from %s", sender);
        }
    }

    /**
     * Merges a received gossip message's membership list with the local membership list.
     * This is a simplified merge logic. In a full SWIM implementation, more complex rules
     * for merging (e.g., handling conflicting statuses for the same epoch) would apply.
     */
    private void processGossip(GossipMessage message) {
        boolean changed = false;
        for (GossipMember receivedMember : message.getMembers().values()) {
            // Ignore self in received list
            if (receivedMember.getNodeId().equals(localNodeInfo.nodeId())) {
                continue;
            }

            GossipMember localMember = members.get(receivedMember.getNodeId());
            if (localMember == null) {
                // New member discovered
                members.put(receivedMember.getNodeId(), receivedMember);
                LOG.infof("Discovered new member via gossip: %s", receivedMember);
                changed = true;
            } else {
                // Update existing member based on epoch and status
                // If received epoch is higher, always update.
                // If epochs are equal, an ALIVE status from remote can override local SUSPECT/DEAD.
                if (receivedMember.getEpoch() > localMember.getEpoch()) {
                    // Received info is strictly newer
                    localMember.setEpoch(receivedMember.getEpoch());
                    localMember.setLastSeen(System.currentTimeMillis());
                    localMember.setStatus(receivedMember.getStatus());
                    LOG.debugf("Updated member %s via gossip to status %s, epoch %d", localMember.getNodeId(), localMember.getStatus(), localMember.getEpoch());
                    changed = true;
                } else if (receivedMember.getEpoch() == localMember.getEpoch()) {
                    // Same epoch, but received member is ALIVE and local is SUSPECT/DEAD
                    if (receivedMember.isAlive() && (localMember.isSuspect() || localMember.isDead())) {
                        localMember.setLastSeen(System.currentTimeMillis());
                        localMember.setStatus(MemberStatus.ALIVE);
                        LOG.infof("Member %s revived via gossip to ALIVE", localMember.getNodeId());
                        changed = true;
                    }
                }
                // If local epoch is higher or equal and local status is ALIVE, we generally prefer our own view.
            }
        }
        if (changed) {
            notifyMembershipChange();
        }
    }

    /**
     * Processes a PING message. Responds with an ACK and merges the sender's membership list.
     */
    private void processPing(GossipMessage message, SocketAddress sender) {
        // Respond with an ACK to the sender
        sendGossipMessage(GossipMessageType.ACK, localNodeInfo.nodeId(), Collections.emptyMap(), sender.host(), sender.port());
        // Also process the membership list included in the PING message
        processGossip(message);
    }

    /**
     * Processes an ACK message. Marks the sender as ALIVE if it was SUSPECT or DEAD.
     */
    private void processAck(GossipMessage message) {
        processGossip(message);
    }

    private void refreshKnownSender(String senderId) {
        GossipMember member = members.get(senderId);
        if (member == null) {
            return;
        }
        boolean statusChanged = !member.isAlive();
        member.setStatus(MemberStatus.ALIVE);
        member.setLastSeen(System.currentTimeMillis());
        if (statusChanged) {
            LOG.infof("Member %s sent a direct gossip message, status set to ALIVE", member.getNodeId());
            notifyMembershipChange();
        }
    }

    /**
     * Processes a PING_REQ message. In a full SWIM implementation, this node would
     * now indirectly ping the target on behalf of the original sender.
     * For this skeleton, we log and process any included gossip.
     */
    private void processPingReq(GossipMessage message, SocketAddress sender) {
        // The PING_REQ message is expected to contain the target member's info in its members map.
        // For simplicity, we assume it's the first entry.
        String targetNodeId = message.getMembers().keySet().stream().findFirst().orElse(null);

        if (targetNodeId == null) {
            LOG.warnf("Received malformed PING_REQ from %s: no target node ID found.", sender);
            processGossip(message); // Still process any other gossip
            return;
        }

        GossipMember targetMember = members.get(targetNodeId);

        // In a full SWIM, we would now send a PING to targetMember and
        // based on its response, send an ACK or NACK to the original sender.
        // For this skeleton, we simplify: if target is ALIVE, we ACK the sender directly.
        // Otherwise, we just process gossip and don't respond (implying failure to sender).
        if (targetMember != null && targetMember.isAlive()) {
            // If the target is known and ALIVE, we can directly ACK the original sender
            sendGossipMessage(GossipMessageType.ACK, localNodeInfo.nodeId(), Collections.emptyMap(), sender.host(), sender.port());
            LOG.debugf("Responded with ACK to PING_REQ from %s for ALIVE target %s", sender, targetNodeId);
        } else {
            LOG.warnf("PING_REQ received from %s for target %s (status: %s). (Simplified: not performing indirect ping)",
                    sender, targetNodeId, targetMember != null ? targetMember.getStatus() : "UNKNOWN");
        }
        processGossip(message); // Process any membership info in the PING_REQ
    }

    /**
     * Selects a random ALIVE or SUSPECT member and sends a PING message.
     */
    private void pingRandomMember() {
        // Select a random ALIVE or SUSPECT member to ping (excluding self)
        Set<GossipMember> potentialTargets = members.values().stream()
                .filter(m -> !m.getNodeId().equals(localNodeInfo.nodeId()))
                .filter(m -> m.isAlive() || m.isSuspect())
                .collect(Collectors.toSet());

        if (potentialTargets.isEmpty()) {
            LOG.debug("No other members to ping.");
            return;
        }

        GossipMember target = potentialTargets.stream()
                .skip((int) (potentialTargets.size() * Math.random())) // Random selection
                .findFirst()
                .orElse(null);

        if (target != null) {
            LOG.debugf("Pinging member %s (%s:%d)", target.getNodeId(), target.getHost(), target.getPort());
            // Send PING message with local membership list (for anti-entropy)
            sendGossipMessage(GossipMessageType.PING, localNodeInfo.nodeId(), members, target.getHost(), gossipConfig.port());
            // In a full SWIM, if no ACK is received within a timeout, we would initiate PING_REQ via other members.
            // For this skeleton, the cleanup timer will eventually mark it SUSPECT/DEAD if no ACK is received.
        }
    }

    /**
     * Selects a random ALIVE member and exchanges gossip (sends its membership list).
     */
    private void exchangeGossip() {
        // Select a random ALIVE member to exchange gossip with (excluding self)
        Set<GossipMember> potentialTargets = members.values().stream()
                .filter(m -> !m.getNodeId().equals(localNodeInfo.nodeId()))
                .filter(GossipMember::isAlive)
                .collect(Collectors.toSet());

        if (potentialTargets.isEmpty()) {
            LOG.debug("No other ALIVE members to exchange gossip with.");
            return;
        }

        GossipMember target = potentialTargets.stream()
                .skip((int) (potentialTargets.size() * Math.random())) // Random selection
                .findFirst()
                .orElse(null);

        if (target != null) {
            LOG.debugf("Exchanging gossip with member %s (%s:%d)", target.getNodeId(), target.getHost(), target.getPort());
            // Send GOSSIP message with a partial or full membership list.
            // For simplicity, this skeleton sends the full local membership list.
            // In a production system, a partial list (e.g., a delta or random subset) would be more efficient.
            sendGossipMessage(GossipMessageType.GOSSIP, localNodeInfo.nodeId(), members, target.getHost(), gossipConfig.port());
        }
    }

    /**
     * Periodically checks members for liveness and updates their status (ALIVE -> SUSPECT -> DEAD).
     * Also cleans up members that have been DEAD for a configured duration.
     */
    private void cleanupDeadMembers() {
        long now = System.currentTimeMillis();
        long failureTimeout = gossipConfig.failureTimeout().toMillis();
        // Duration a member stays SUSPECT before becoming DEAD.
        // In a full SWIM, this might be a separate configurable value or derived from failureTimeout.
        long suspectToDeadTimeout = failureTimeout;
        long deadMemberRemovalDelay = gossipConfig.deadMemberCleanupDelay().toMillis(); // Time to keep DEAD members before removal

        boolean statusChanged = false; // Renamed for clarity and to avoid lambda capture issues
        for (GossipMember member : members.values()) {
            if (member.getNodeId().equals(localNodeInfo.nodeId())) {
                continue; // Don't process self for failure detection
            }

            if (member.isAlive()) {
                if (now - member.getLastSeen() > failureTimeout) {
                    // Member hasn't been seen for failureTimeout, mark as SUSPECT
                    member.setStatus(MemberStatus.SUSPECT);
                    member.setLastSeen(now); // IMPORTANT: Update lastSeen to 'now' to start timing for SUSPECT duration
                    LOG.warnf("Member %s marked as SUSPECT due to no recent updates (last seen %d ms ago)", member.getNodeId(), (now - member.getLastSeen()));
                    statusChanged = true;
                }
            } else if (member.isSuspect()) {
                if (now - member.getLastSeen() > suspectToDeadTimeout) { // If SUSPECT for 'suspectToDeadTimeout' duration
                    // Member was SUSPECT and still no update, mark as DEAD
                    member.setStatus(MemberStatus.DEAD);
                    member.setLastSeen(now); // IMPORTANT: Update lastSeen to 'now' to start timing for DEAD removal
                    LOG.errorf("Member %s marked as DEAD due to prolonged unresponsiveness (last seen %d ms ago)", member.getNodeId(), (now - member.getLastSeen()));
                    statusChanged = true;
                }
            }
            // DEAD members are handled by the removeIf block below
        }

        // Remove DEAD members from the map after their removal delay
        // The removeIf method returns true if any elements were removed.
        boolean removedFromMap = members.entrySet().removeIf(entry -> {
            GossipMember member = entry.getValue();
            // Only remove if DEAD and has been DEAD for longer than deadMemberRemovalDelay
            // The lastSeen timestamp is updated when status changes, so this works.
            if (member.isDead() && (now - member.getLastSeen() > deadMemberRemovalDelay)) {
                LOG.infof("Cleaning up DEAD member %s from membership list", member.getNodeId());
                return true; // This element should be removed
            }
            return false;
        });

        if (statusChanged || removedFromMap) { // Notify if any status changed or members were removed
            notifyMembershipChange();
        }
    }

    /**
     * Serializes a GossipMessage and sends it via UDP to the target host and port.
     */
    private void sendGossipMessage(GossipMessageType type, String senderId, Map<String, GossipMember> membersToSend, String targetHost, int targetPort) {
        try {
            GossipMessage message = new GossipMessage(type, senderId, membersToSend);
            Buffer buffer = Buffer.buffer(GossipMessageCodec.encode(message));

            socket.send(buffer, targetPort, targetHost)
                    .onFailure(e -> LOG.warnf(e, "Failed to send gossip message %s from %s to %s:%d", type, senderId, targetHost, targetPort));
        } catch (IOException e) {
            LOG.warnf(e, "Failed to encode gossip message %s from %s", type, senderId);
        }
    }

    @Override
    public void announce() {
        // In gossip, announcing is implicitly handled by periodic gossip exchange and pings.
        // The local node is added to the membership list in start() and its status is propagated.
        // For initial bootstrapping, seedMembership() attempts to connect to known seeds.
        LOG.debug("GossipDiscoveryStrategy: Announce called, implicitly handled by periodic gossip.");
    }

    @Override
    public Set<NodeInfo> getDiscoveredNodes() {
        // Return an immutable set of currently ALIVE nodes.
        return members.values().stream()
                .filter(GossipMember::isAlive)
                .map(GossipMember::getNodeInfo)
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Notifies the registered membership listener if the set of active (ALIVE) nodes has changed.
     * This prevents unnecessary updates to CoordinationService for minor internal state changes.
     */
    private void notifyMembershipChange() {
        if (membershipListener == null) {
            return;
        }

        Set<NodeInfo> currentActiveNodes = getDiscoveredNodes();
        if (!currentActiveNodes.equals(lastKnownActiveNodes)) {
            LOG.infof("Membership change detected. Notifying listener. Old active nodes: %s, New active nodes: %s", lastKnownActiveNodes, currentActiveNodes);
            membershipListener.accept(currentActiveNodes);
            lastKnownActiveNodes = currentActiveNodes;
        }
    }

    @Override
    public void close() {
        // Cancel all periodic timers
        if (pingTimerId != -1) {
            vertx.cancelTimer(pingTimerId);
        }
        if (gossipTimerId != -1) {
            vertx.cancelTimer(gossipTimerId);
        }
        if (cleanupTimerId != -1) {
            vertx.cancelTimer(cleanupTimerId);
        }
        // Close the UDP socket
        if (socket != null) {
            socket.close();
        }
        LOG.info("GossipDiscoveryStrategy closed.");
    }
}
