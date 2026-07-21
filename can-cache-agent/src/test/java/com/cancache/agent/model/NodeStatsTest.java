package com.cancache.agent.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeStatsTest {

    @Test
    void recordsProbeLatencyAndHealthHistory() {
        NodeStats node = new NodeStats("10.0.0.4:11212");
        var initialStateChange = node.lastStateChange();

        node.recordHealthCheck(UpstreamState.UP, null, 7L);
        node.recordHealthCheck(UpstreamState.DOWN, "connection refused", 19L);

        assertEquals(UpstreamState.DOWN, node.state());
        assertEquals(19L, node.lastLatencyMillis());
        assertEquals(1L, node.successfulChecks());
        assertEquals(1L, node.failedChecks());
        assertEquals("connection refused", node.lastError());
        assertFalse(node.lastCheck().equals(java.time.Instant.EPOCH));
        assertTrue(!node.lastStateChange().isBefore(initialStateChange));
    }

    @Test
    void tracksConnectionAndTrafficCounters() {
        NodeStats node = new NodeStats("cache-0:11212");

        node.incActiveConn();
        node.addBytesIn(128L);
        node.addBytesOut(64L);
        node.decActiveConn();

        assertEquals(0, node.activeConn());
        assertEquals(1L, node.totalConn());
        assertEquals(128L, node.bytesIn());
        assertEquals(64L, node.bytesOut());
    }
}
