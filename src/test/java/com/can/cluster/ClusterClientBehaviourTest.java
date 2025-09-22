package com.can.cluster;

import com.can.codec.StringCodec;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Küme yapısı sabit hash ile replikalar seçer ve ClusterClient istekleri ilgili düğümlere yayınlar.
 * Testler hem halka mantığını hem de istemci delegasyonunu adım adım açıklar.
 */
class ClusterClientBehaviourTest {

    private static final HashFn SIMPLE_HASH = key -> java.util.Arrays.hashCode(key);

    @Nested
    class RingSelection {
        /**
         * ConsistentHashRing replikaları istenen sayıda benzersiz düğüm olarak döndürür.
         * Hash sırasının sonunda wrap-around yaparak listeyi tamamlar.
         */
        @Test
        void returnsReplicasInRingOrder() {
            ConsistentHashRing<String> ring = new ConsistentHashRing<>(SIMPLE_HASH, 3);
            ring.addNode("node-1", "node-1".getBytes());
            ring.addNode("node-2", "node-2".getBytes());
            ring.addNode("node-3", "node-3".getBytes());

            List<String> replicas = ring.getReplicas("key".getBytes(), 2);
            assertEquals(2, replicas.size());
            assertTrue(replicas.contains("node-1") || replicas.contains("node-2") || replicas.contains("node-3"));

            List<String> more = ring.getReplicas("key".getBytes(), 5);
            assertEquals(3, more.size(), "Toplam düğüm sayısından fazla istek benzersiz düğümler döndürmeli");
        }
    }

    @Nested
    class ClientDelegation {
        /**
         * ClusterClient aynı anahtarı tüm replikalara gönderir; get ilk başarılı yanıtı döndürür.
         * delete ise herhangi bir düğümde başarılı olursa true döndürmelidir.
         */
        @Test
        void replicatesOperationsAcrossNodes() {
            ConsistentHashRing<Node<String, String>> ring = new ConsistentHashRing<>(SIMPLE_HASH, 1);
            RecordingNode nodeA = new RecordingNode("A");
            RecordingNode nodeB = new RecordingNode("B");
            ring.addNode(nodeA, nodeA.id().getBytes());
            ring.addNode(nodeB, nodeB.id().getBytes());

            ClusterClient<String, String> client = new ClusterClient<>(ring, 2, StringCodec.UTF8);

            client.set("k", "v", Duration.ofSeconds(1));
            assertEquals(List.of("k"), nodeA.setKeys);
            assertEquals(List.of("k"), nodeB.setKeys);

            nodeA.value = null;
            nodeB.value = "v";
            assertEquals("v", client.get("k"));

            nodeA.deleteResult = false;
            nodeB.deleteResult = true;
            assertTrue(client.delete("k"));
        }
    }

    private static final class RecordingNode implements Node<String, String> {
        private final String id;
        private final List<String> setKeys = new CopyOnWriteArrayList<>();
        private volatile String value;
        private volatile boolean deleteResult;

        private RecordingNode(String id) {
            this.id = id;
        }

        @Override
        public boolean set(String key, String value, Duration ttl) {
            setKeys.add(key);
            this.value = value;
            return true;
        }

        @Override
        public String get(String key) {
            return value;
        }

        @Override
        public boolean delete(String key) {
            return deleteResult;
        }

        @Override
        public boolean compareAndSwap(String key, String value, long expectedCas, Duration ttl) {
            this.value = value;
            return true;
        }

        @Override
        public String id() {
            return id;
        }
    }
}
