package com.cancache.agent.cluster.coordination.gossip;

public enum GossipMessageType {
    // A regular gossip message containing a partial or full membership list
    GOSSIP,
    // A direct ping message to check liveness
    PING,
    // A response to a ping message
    ACK,
    // A request to indirectly ping a suspect member
    PING_REQ
}