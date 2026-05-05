package com.cancache.agent.cluster.coordination.gossip;

import com.cancache.agent.cluster.coordination.NodeInfo;

import java.io.Serializable;
import java.util.Objects;

public class GossipMember implements Serializable { // Added Serializable
    private static final long serialVersionUID = 1L; // Added serialVersionUID

    private final NodeInfo nodeInfo;
    private volatile long epoch;
    private volatile long lastSeen; // Timestamp of last update from this member
    private volatile MemberStatus status; // ALIVE, SUSPECT, DEAD

    public GossipMember(NodeInfo nodeInfo, long epoch, long lastSeen, MemberStatus status) {
        this.nodeInfo = nodeInfo;
        this.epoch = epoch;
        this.lastSeen = lastSeen;
        this.status = status;
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    public String getNodeId() {
        return nodeInfo.nodeId();
    }

    public String getHost() {
        return nodeInfo.host();
    }

    public int getPort() {
        return nodeInfo.port();
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    public MemberStatus getStatus() {
        return status;
    }

    public void setStatus(MemberStatus status) {
        this.status = status;
    }

    public boolean isAlive() {
        return status == MemberStatus.ALIVE;
    }

    public boolean isSuspect() {
        return status == MemberStatus.SUSPECT;
    }

    public boolean isDead() {
        return status == MemberStatus.DEAD;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GossipMember that = (GossipMember) o;
        return nodeInfo.equals(that.nodeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeInfo);
    }

    @Override
    public String toString() {
        return "GossipMember{" +
               "nodeInfo=" + nodeInfo +
               ", epoch=" + epoch +
               ", status=" + status +
               '}';
    }
}