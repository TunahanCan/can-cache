package com.cancache.agent.cluster.coordination.gossip;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class GossipMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final GossipMessageType type;
    private final String senderId;
    private final Map<String, GossipMember> members; // Full or partial membership list

    public GossipMessage(GossipMessageType type, String senderId, Map<String, GossipMember> members) {
        this.type = type;
        this.senderId = senderId;
        this.members = members;
    }

    public GossipMessageType getType() {
        return type;
    }

    public String getSenderId() {
        return senderId;
    }

    public Map<String, GossipMember> getMembers() {
        return members;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GossipMessage that = (GossipMessage) o;
        return type == that.type && senderId.equals(that.senderId) && members.equals(that.members);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, senderId, members);
    }

    @Override
    public String toString() {
        return "GossipMessage{" +
               "type=" + type +
               ", senderId='" + senderId + '\'' +
               ", members=" + members +
               '}';
    }
}