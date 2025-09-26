package com.can.cluster;

import com.can.metric.MetricsRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class HintedHandoffServiceTest
{
    private MetricsRegistry metrics;
    private HintedHandoffService service;
    private FakeNode node;

    @BeforeEach
    void setup()
    {
        metrics = new MetricsRegistry();
        service = new HintedHandoffService(metrics);
        node = new FakeNode();
    }

    @Nested
    class RecordingOperations
    {
        // Bu test set ipucunun kuyruğa eklendiğini ve metriklerin arttığını doğrular.
        @Test
        void record_set_enqueues_hint()
        {
            service.recordSet("node", "key", "value", Duration.ofSeconds(1));
            assertEquals(1, service.pendingFor("node"));
            assertEquals(1L, metrics.counter("hinted_handoff_enqueued_total").get());
        }

        // Bu test delete ve CAS ipuçlarının da kuyruğa eklendiğini gösterir.
        @Test
        void record_delete_and_cas_enqueue_hints()
        {
            service.recordDelete("node", "key");
            service.recordCas("node", "key", "value", 5L, Duration.ZERO);
            assertEquals(2, service.pendingFor("node"));
        }
    }

    @Nested
    class ReplayOperations
    {
        // Bu test başarılı ipuçlarının yeniden oynatılarak kuyruktan silindiğini doğrular.
        @Test
        void replay_cleans_up_successful_hints()
        {
            service.recordSet("node", "key", "value", Duration.ofSeconds(1));
            service.recordDelete("node", "key");
            service.recordCas("node", "key", "value", 1L, Duration.ofSeconds(1));
            service.replay("node", node);
            assertEquals(1, node.setCallCount());
            assertEquals(1, node.deleteCallCount());
            assertEquals(1, node.casCallCount());
            assertEquals(0, service.pendingFor("node"));
            assertEquals(3L, metrics.counter("hinted_handoff_replayed_total").get());
        }

        // Bu test yeniden oynatma başarısız olduğunda ipucunun kuyrukta kaldığını ve hata metriğinin arttığını doğrular.
        @Test
        void replay_leaves_hint_on_failure()
        {
            service.recordSet("node", "key", "value", Duration.ZERO);
            node.throwNextSet();
            service.replay("node", node);
            assertEquals(1, node.setCallCount());
            assertEquals(1, service.pendingFor("node"));
            assertEquals(1L, metrics.counter("hinted_handoff_failures_total").get());
        }
    }

    private static final class FakeNode implements Node<String, String>
    {
        private boolean throwSet;
        private int setCalls;
        private int deleteCalls;
        private int casCalls;

        void throwNextSet()
        {
            this.throwSet = true;
        }

        int setCallCount()
        {
            return setCalls;
        }

        int deleteCallCount()
        {
            return deleteCalls;
        }

        int casCallCount()
        {
            return casCalls;
        }

        @Override
        public boolean set(String key, String value, Duration ttl)
        {
            setCalls++;
            if (throwSet)
            {
                throwSet = false;
                throw new RuntimeException("set-fail");
            }
            return true;
        }

        @Override
        public String get(String key)
        {
            return null;
        }

        @Override
        public boolean delete(String key)
        {
            deleteCalls++;
            return true;
        }

        @Override
        public boolean compareAndSwap(String key, String value, long expectedCas, Duration ttl)
        {
            casCalls++;
            return true;
        }

        @Override
        public void clear()
        {
        }

        @Override
        public String id()
        {
            return "fake";
        }
    }
}
