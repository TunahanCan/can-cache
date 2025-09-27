package com.can.cluster;

import com.can.codec.StringCodec;
import com.can.metric.MetricsRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ClusterClientTest
{
    private ConsistentHashRing<Node<String, String>> ring;
    private HintedHandoffService handoff;
    private ClusterClient client;
    private FakeNode leader;
    private FakeNode replica1;
    private FakeNode replica2;

    @BeforeEach
    void setup()
    {
        handoff = new HintedHandoffService(new MetricsRegistry());
        ring = new ConsistentHashRing<>(new ControlledHash(), 1);
        leader = new FakeNode("leader");
        replica1 = new FakeNode("replica1");
        replica2 = new FakeNode("replica2");
        ring.addNode(leader, bytes("leader"));
        ring.addNode(replica1, bytes("replica1"));
        ring.addNode(replica2, bytes("replica2"));
        client = new ClusterClient(ring, 3, StringCodec.UTF8, handoff);
    }

    @Nested
    class SetOperations
    {
        // Bu test halka boş olduğunda set çağrısının başarısız döndüğünü doğrular.
        @Test
        void set_returns_false_on_empty_ring()
        {
            ConsistentHashRing<Node<String, String>> emptyRing = new ConsistentHashRing<>(new ControlledHash(), 1);
            ClusterClient emptyClient = new ClusterClient(emptyRing, 3, StringCodec.UTF8, handoff);
            assertFalse(emptyClient.set("clientKey", "value", null));
        }

        // Bu test çoğunluk başarı sağladığında true döndüğünü ve hatalı düğümün kuyruğa eklendiğini gösterir.
        @Test
        void set_returns_true_when_quorum_reached()
        {
            replica1.failNextSet();
            assertTrue(client.set("clientKey", "value", Duration.ofSeconds(1)));
            assertEquals(1, handoff.pendingFor(replica1.id()));
            assertEquals(0, handoff.pendingFor(replica2.id()));
            assertEquals(0, handoff.pendingFor(leader.id()));
        }

        // Bu test lider düğüm hata verip çoğunluk sağlanamadığında istisna fırlatıldığını doğrular.
        @Test
        void set_throws_when_leader_fails_and_no_quorum()
        {
            leader.throwNextSet();
            replica1.failNextSet();
            replica2.failNextSet();
            RuntimeException ex = assertThrows(RuntimeException.class, () -> client.set("clientKey", "value", null));
            assertTrue(ex.getMessage().contains("set"));
            assertEquals(1, handoff.pendingFor(leader.id()));
            assertEquals(1, handoff.pendingFor(replica1.id()));
            assertEquals(1, handoff.pendingFor(replica2.id()));
        }
    }

    @Nested
    class ReadOperations
    {
        // Bu test ilk başarılı replikanın değerini döndürdüğünü doğrular.
        @Test
        void get_returns_value_from_first_successful_replica()
        {
            replica1.preset("value");
            assertEquals("value", client.get("clientKey"));
        }

        // Bu test hiçbir replika değer döndürmezse null geldiğini gösterir.
        @Test
        void get_returns_null_when_all_replicas_empty()
        {
            assertNull(client.get("clientKey"));
        }
    }

    @Nested
    class DeleteOperations
    {
        // Bu test iki replika silmeyi başarıyla tamamladığında true döndüğünü doğrular.
        @Test
        void delete_returns_true_with_quorum()
        {
            replica2.failNextDelete();
            assertTrue(client.delete("clientKey"));
            assertEquals(1, handoff.pendingFor(replica2.id()));
        }

        // Bu test çoğunluk sağlanamadığında false döndüğünü ve ipucu kaydedildiğini gösterir.
        @Test
        void delete_returns_false_without_quorum()
        {
            leader.failNextDelete();
            replica1.failNextDelete();
            assertFalse(client.delete("clientKey"));
            assertEquals(1, handoff.pendingFor(replica1.id()));
            assertEquals(0, handoff.pendingFor(replica2.id()));
        }
    }

    @Nested
    class CasOperations
    {
        // Bu test çoğunluk sağlandığında CAS operasyonunun true döndürdüğünü doğrular.
        @Test
        void compare_and_swap_returns_true_with_quorum()
        {
            replica2.failNextCas();
            assertTrue(client.compareAndSwap("clientKey", "v", 1L, Duration.ofSeconds(1)));
            assertEquals(0, handoff.pendingFor(replica2.id()));
        }

        // Bu test lider hata verdiğinde ve çoğunluk sağlanamadığında istisna fırlatıldığını gösterir.
        @Test
        void compare_and_swap_throws_when_leader_fails()
        {
            leader.throwNextCas();
            replica1.failNextCas();
            replica2.failNextCas();
            RuntimeException ex = assertThrows(RuntimeException.class, () -> client.compareAndSwap("clientKey", "v", 1L, null));
            assertTrue(ex.getMessage().contains("cas"));
            assertEquals(1, handoff.pendingFor(leader.id()));
        }
    }

    @Nested
    class MaintenanceOperations
    {
        // Bu test clear çağrısının tüm düğümlerde yürütüldüğünü doğrular.
        @Test
        void clear_invokes_all_nodes()
        {
            client.clear();
            assertEquals(1, leader.clearCalls);
            assertEquals(1, replica1.clearCalls);
            assertEquals(1, replica2.clearCalls);
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
                case "leader" -> 100 + vnode;
                case "replica1" -> 200 + vnode;
                case "replica2" -> 300 + vnode;
                case "clientKey" -> 50;
                default -> text.hashCode();
            };
        }
    }

    private static final class FakeNode implements Node<String, String>
    {
        private final String id;
        private boolean failSet;
        private boolean throwSet;
        private boolean failDelete;
        private boolean throwDelete;
        private boolean failCas;
        private boolean throwCas;
        private String storedValue;
        private int clearCalls;

        FakeNode(String id)
        {
            this.id = id;
        }

        void failNextSet()
        {
            this.failSet = true;
        }

        void throwNextSet()
        {
            this.throwSet = true;
        }

        void failNextDelete()
        {
            this.failDelete = true;
        }

        void failNextCas()
        {
            this.failCas = true;
        }

        void throwNextCas()
        {
            this.throwCas = true;
        }

        void preset(String value)
        {
            this.storedValue = value;
        }

        @Override
        public boolean set(String key, String value, Duration ttl)
        {
            if (throwSet)
            {
                throwSet = false;
                throw new RuntimeException("set-fail-" + id);
            }
            if (failSet)
            {
                failSet = false;
                return false;
            }
            storedValue = value;
            return true;
        }

        @Override
        public String get(String key)
        {
            return storedValue;
        }

        @Override
        public boolean delete(String key)
        {
            if (throwDelete)
            {
                throwDelete = false;
                throw new RuntimeException("delete-fail-" + id);
            }
            if (failDelete)
            {
                failDelete = false;
                return false;
            }
            storedValue = null;
            return true;
        }

        @Override
        public boolean compareAndSwap(String key, String value, long expectedCas, Duration ttl)
        {
            if (throwCas)
            {
                throwCas = false;
                throw new RuntimeException("cas-fail-" + id);
            }
            if (failCas)
            {
                failCas = false;
                return false;
            }
            storedValue = value;
            return true;
        }

        @Override
        public void clear()
        {
            clearCalls++;
            storedValue = null;
        }

        @Override
        public String id()
        {
            return id;
        }
    }
}
