package com.can.cluster;

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
        // Bu test düğüm eklendiğinde benzersiz liste olarak döndürüldüğünü doğrular.
        @Test
        void add_node_returns_unique_list()
        {
            ring.addNode("A", bytes("A"));
            ring.addNode("B", bytes("B"));
            assertEquals(List.of("A", "B"), ring.nodes());
        }

        // Bu test düğüm kaldırıldığında replika listesinden çıkarıldığını gösterir.
        @Test
        void remove_node_clears_virtual_nodes()
        {
            ring.addNode("A", bytes("A"));
            ring.addNode("B", bytes("B"));
            ring.removeNode("B", bytes("B"));
            assertEquals(List.of("A"), ring.nodes());
            assertEquals(List.of("A"), ring.getReplicas(bytes("key"), 2));
        }
    }

    @Nested
    class ReplicaSelection
    {
        // Bu test replikaların belirlenen sırada ve tekrar etmeden döndüğünü doğrular.
        @Test
        void get_replicas_returns_requested_nodes_in_order()
        {
            ring.addNode("A", bytes("A"));
            ring.addNode("B", bytes("B"));
            ring.addNode("C", bytes("C"));
            List<String> replicas = ring.getReplicas(bytes("key"), 3);
            assertEquals(List.of("A", "B", "C"), replicas);
        }

        // Bu test istenen replika sayısından az düğüm olduğunda mevcut düğümlerin döndürüldüğünü gösterir.
        @Test
        void get_replicas_limits_to_available_nodes()
        {
            ring.addNode("A", bytes("A"));
            ring.addNode("B", bytes("B"));
            List<String> replicas = ring.getReplicas(bytes("key"), 5);
            assertEquals(List.of("A", "B"), replicas);
        }

        // Bu test boş halkada replika isteğinin boş liste ürettiğini doğrular.
        @Test
        void get_replicas_returns_empty_list_for_empty_ring()
        {
            assertTrue(ring.getReplicas(bytes("key"), 2).isEmpty());
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
}
