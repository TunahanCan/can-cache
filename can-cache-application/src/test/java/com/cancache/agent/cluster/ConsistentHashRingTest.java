package com.cancache.agent.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashRingTest
{
    private ConsistentHashRing<String> ring;

    @BeforeEach
    void setup()
    {
        ring = new ConsistentHashRing<>(new ControlledHash(), 3);
    }

    @Nested
    class NodeManagement
    {
        /**
         * Verifies that when nodes are added, they are returned as a unique list.
         */
        @Test
        void shouldReturnUniqueListWhenNodesAdded()
        {
            // Given / When
            ring.addNode("A", bytes("A"));
            ring.addNode("B", bytes("B"));
            
            // Then
            assertEquals(List.of("A", "B"), ring.nodes(), "Added nodes should be present in the ring");
        }

        /**
         * Shows that when a node is removed, it is omitted from the replica list.
         */
        @Test
        void shouldClearVirtualNodesWhenNodeRemoved()
        {
            // Given
            ring.addNode("A", bytes("A"));
            ring.addNode("B", bytes("B"));
            
            // When
            ring.removeNode("B", bytes("B"));
            
            // Then
            assertEquals(List.of("A"), ring.nodes(), "Removed node should no longer be present");
            assertEquals(List.of("A"), ring.getReplicas(bytes("key"), 2), "Removed node should not be returned as a replica");
        }

        @Test
        void shouldPreserveCollidingVirtualNodesAndIgnoreStaleRemoval()
        {
            ConsistentHashRing<TestNode> collidingRing = new ConsistentHashRing<>(ignored -> 7, 2);
            TestNode oldA = new TestNode("A", 1);
            TestNode replacementA = new TestNode("A", 2);
            TestNode nodeB = new TestNode("B", 1);

            collidingRing.addNode(oldA, bytes("A"));
            collidingRing.addNode(nodeB, bytes("B"));
            collidingRing.addNode(replacementA, bytes("A"));
            collidingRing.removeNode(oldA, bytes("A"));

            assertEquals(List.of(replacementA, nodeB), collidingRing.getReplicas(bytes("key"), 2),
                    "Hash collisions and stale removals must not erase the current owner");
        }
    }

    @Nested
    class ReplicaSelection
    {
        /**
         * Verifies that replicas are returned in the determined order without duplicates.
         */
        @Test
        void shouldReturnRequestedNodesInOrderOnGetReplicas()
        {
            // Given
            ring.addNode("A", bytes("A"));
            ring.addNode("B", bytes("B"));
            ring.addNode("C", bytes("C"));
            
            // When
            List<String> replicas = ring.getReplicas(bytes("key"), 3);
            
            // Then
            assertEquals(List.of("A", "B", "C"), replicas, "Replicas should be returned in expected order based on consistent hashing");
        }

        /**
         * Shows that when there are fewer nodes than requested, all available nodes are returned.
         */
        @Test
        void shouldLimitToAvailableNodesOnGetReplicas()
        {
            // Given
            ring.addNode("A", bytes("A"));
            ring.addNode("B", bytes("B"));
            
            // When
            List<String> replicas = ring.getReplicas(bytes("key"), 5);
            
            // Then
            assertEquals(List.of("A", "B"), replicas, "Returned replicas should not exceed available nodes");
        }

        /**
         * Verifies that requesting replicas from an empty ring produces an empty list.
         */
        @Test
        void shouldReturnEmptyListForEmptyRingOnGetReplicas()
        {
            // Given (empty ring)

            // When
            List<String> replicas = ring.getReplicas(bytes("key"), 2);
            
            // Then
            assertTrue(replicas.isEmpty(), "Empty list should be returned when there are no nodes in the ring");
        }
    }

    private static byte[] bytes(String value)
    {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class ControlledHash implements HashFn
    {
        @Override
        public int hash(byte[] keyBytes)
        {
            String text = new String(keyBytes, StandardCharsets.UTF_8);
            int vnode = 0;
            int idx = text.indexOf('#');
            if (idx >= 0)
            {
                vnode = Integer.parseInt(text.substring(idx + 1));
                text = text.substring(0, idx);
            }
            return switch (text)
            {
                case "A" -> 100 + vnode;
                case "B" -> 200 + vnode;
                case "C" -> 300 + vnode;
                case "key" -> 50;
                default -> text.hashCode();
            };
        }
    }

    private record TestNode(String id, int generation)
    {
    }
}
