package com.cancache.agent.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeStatsReliabilityTest {

    @Test
    void loadShouldIncludePendingAndActiveConnectionsWithoutUnderflowing() {
        NodeStats node = new NodeStats("cache-a:11212");

        assertEquals(1, node.reservePendingConnection());
        assertEquals(2, node.reservePendingConnection());
        assertEquals(1, node.incActiveConn());
        assertEquals(2, node.pendingConn());
        assertEquals(3, node.load());
        assertEquals(1, node.totalConn());

        assertEquals(1, node.releasePendingConnection());
        assertEquals(0, node.releasePendingConnection());
        assertEquals(0, node.releasePendingConnection());
        assertEquals(1, node.load());

        assertEquals(0, node.decActiveConn());
        assertEquals(0, node.decActiveConn());
        assertEquals(0, node.activeConn());
        assertEquals(0, node.load());
        assertEquals(1, node.totalConn());
    }

    @Test
    void passiveFailuresShouldEjectOnlyAtThresholdAndResetAfterSuccess() {
        NodeStats node = new NodeStats("cache-a:11212");
        node.state(UpstreamState.UP);

        assertFalse(node.recordPassiveFailure(3));
        assertFalse(node.recordPassiveFailure(3));
        assertEquals(UpstreamState.UP, node.state());

        assertTrue(node.recordPassiveFailure(3));
        assertEquals(UpstreamState.DOWN, node.state());

        node.clearPassiveFailures();
        node.state(UpstreamState.UP);
        assertFalse(node.recordPassiveFailure(2));
        assertEquals(UpstreamState.UP, node.state());
        assertTrue(node.recordPassiveFailure(2));
        assertEquals(UpstreamState.DOWN, node.state());
    }

    @Test
    void nonPositivePassiveFailureThresholdShouldFailClosed() {
        NodeStats node = new NodeStats("cache-a:11212");
        node.state(UpstreamState.UP);

        assertTrue(node.recordPassiveFailure(0));
        assertEquals(UpstreamState.DOWN, node.state());
    }
}
