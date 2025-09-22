package com.can.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashRingTest
{
    private ConsistentHashRing<String> ring;

    @BeforeEach
    void setup()
    {
        ring = new ConsistentHashRing<>(bytes -> Arrays.hashCode(bytes), 4);
    }

    @Nested
    class NodeManagement
    {
        @Test
        void add_node_registers_unique_entries()
        {
            ring.addNode("n1", "n1".getBytes());
            ring.addNode("n2", "n2".getBytes());

            List<String> nodes = ring.nodes();
            assertTrue(nodes.contains("n1"));
            assertTrue(nodes.contains("n2"));
        }

        @Test
        void remove_node_removes_virtual_nodes()
        {
            ring.addNode("n1", "n1".getBytes());
            ring.addNode("n2", "n2".getBytes());

            ring.removeNode("n1", "n1".getBytes());
            List<String> nodes = ring.nodes();
            assertFalse(nodes.contains("n1"));
        }
    }

    @Nested
    class ReplicaSelection
    {
        @Test
        void get_replicas_returns_requested_number()
        {
            ring.addNode("n1", "n1".getBytes());
            ring.addNode("n2", "n2".getBytes());
            ring.addNode("n3", "n3".getBytes());

            List<String> replicas = ring.getReplicas("key".getBytes(), 2);
            assertEquals(2, replicas.size());
        }

        @Test
        void get_replicas_wraps_around_ring()
        {
            ring.addNode("n1", "n1".getBytes());
            ring.addNode("n2", "n2".getBytes());

            List<String> replicas = ring.getReplicas("key".getBytes(), 5);
            assertTrue(replicas.size() <= 2);
            assertTrue(replicas.contains("n1"));
            assertTrue(replicas.contains("n2"));
        }
    }
}
