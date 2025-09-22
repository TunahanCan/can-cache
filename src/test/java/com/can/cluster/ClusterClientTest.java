package com.can.cluster;

import com.can.codec.StringCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class ClusterClientTest
{
    private ConsistentHashRing<Node<String, String>> ring;
    private TestNode node1;
    private TestNode node2;
    private ClusterClient<String, String> client;

    @BeforeEach
    void setup()
    {
        ring = new ConsistentHashRing<>(bytes -> Arrays.hashCode(bytes), 3);
        node1 = new TestNode("node1");
        node2 = new TestNode("node2");
        ring.addNode(node1, node1.id().getBytes());
        ring.addNode(node2, node2.id().getBytes());
        client = new ClusterClient<>(ring, 2, StringCodec.UTF8);
    }

    @Nested
    class SetGetBehaviour
    {
        @Test
        void set_writes_to_all_replicas()
        {
            assertTrue(client.set("key", "value", Duration.ofSeconds(1)));
            assertEquals("value", node1.get("key"));
            assertEquals("value", node2.get("key"));
        }

        @Test
        void get_returns_first_available_value()
        {
            node1.set("key", "value", null);
            assertEquals("value", client.get("key"));
        }
    }

    @Nested
    class DeleteBehaviour
    {
        @Test
        void delete_returns_true_when_any_node_removes()
        {
            client.set("key", "value", null);
            assertTrue(client.delete("key"));
            assertNull(node1.get("key"));
        }
    }

    @Nested
    class CasBehaviour
    {
        @Test
        void compare_and_swap_requires_all_success()
        {
            client.set("key", "value", null);
            long expected = node1.cas("key");
            assertTrue(client.compareAndSwap("key", "new", expected, null));
            assertEquals("new", node1.get("key"));
            assertEquals("new", node2.get("key"));
        }

        @Test
        void compare_and_swap_fails_when_any_node_fails()
        {
            client.set("key", "value", null);
            node2.forceCas("key", 999L);
            long expected = node1.cas("key");
            assertFalse(client.compareAndSwap("key", "new", expected, null));
            assertEquals("value", node1.get("key"));
        }
    }

    @Nested
    class ClearBehaviour
    {
        @Test
        void clear_invokes_all_nodes()
        {
            client.set("a", "1", null);
            client.set("b", "2", null);
            client.clear();

            assertTrue(node1.isEmpty());
            assertTrue(node2.isEmpty());
        }
    }

    private static final class TestNode implements Node<String, String>
    {
        private final String id;
        private final Map<String, Entry> store = new ConcurrentHashMap<>();
        private long casCounter;

        private TestNode(String id)
        {
            this.id = id;
        }

        @Override
        public boolean set(String key, String value, Duration ttl)
        {
            store.put(key, new Entry(value, ++casCounter));
            return true;
        }

        @Override
        public String get(String key)
        {
            Entry entry = store.get(key);
            return entry == null ? null : entry.value;
        }

        @Override
        public boolean delete(String key)
        {
            return store.remove(key) != null;
        }

        @Override
        public boolean compareAndSwap(String key, String value, long expectedCas, Duration ttl)
        {
            Entry entry = store.get(key);
            if (entry == null || entry.cas != expectedCas)
            {
                return false;
            }
            store.put(key, new Entry(value, ++casCounter));
            return true;
        }

        @Override
        public void clear()
        {
            store.clear();
        }

        @Override
        public String id()
        {
            return id;
        }

        long cas(String key)
        {
            Entry entry = store.get(key);
            return entry == null ? 0L : entry.cas;
        }

        void forceCas(String key, long cas)
        {
            Entry entry = store.get(key);
            if (entry != null)
            {
                store.put(key, new Entry(entry.value, cas));
            }
        }

        boolean isEmpty()
        {
            return store.isEmpty();
        }

        private record Entry(String value, long cas) {}
    }
}
