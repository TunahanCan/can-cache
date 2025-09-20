package com.can.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class ConsistentHashRingTest {

    private static final HashFn SIMPLE_HASH = key -> Arrays.hashCode(key);

    @Test
    void returnsAvailableNodesWhenReplicationFactorExceedsUniqueNodes() {
        ConsistentHashRing<String> ring = new ConsistentHashRing<>(SIMPLE_HASH, 3);
        ring.addNode("node-1", "node-1".getBytes(StandardCharsets.UTF_8));

        List<String> replicas = ring.getReplicas("key".getBytes(StandardCharsets.UTF_8), 5);

        assertEquals(1, replicas.size(), "Only available nodes should be returned");
        assertIterableEquals(List.of("node-1"), replicas);
    }
}
