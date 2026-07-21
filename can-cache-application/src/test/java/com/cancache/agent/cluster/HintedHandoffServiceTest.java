package com.cancache.agent.cluster;

import com.cancache.agent.metric.MetricsRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
        /**
         * Verifies that recording a set hint enqueues it and increments the corresponding metric.
         */
        @Test
        void shouldEnqueueHintOnRecordSet()
        {
            // Given / When
            service.recordSet("node", "key", "value", Duration.ofSeconds(1));
            
            // Then
            assertEquals(1, service.pendingFor("node"), "Pending hints for node should be 1");
            assertEquals(1L, metrics.counter("hinted_handoff_enqueued_total").get(), "Enqueued hints counter should be incremented");
        }

        /**
         * Shows that recording delete and CAS hints also enqueues them properly.
         */
        @Test
        void shouldEnqueueHintsOnRecordDeleteAndCas()
        {
            // Given / When
            service.recordDelete("node", "key");
            service.recordCas("node", "key", "value", 5L, Duration.ZERO);
            
            // Then
            assertEquals(2, service.pendingFor("node"), "Pending hints for node should be 2 after delete and CAS records");
        }

        @Test
        void shouldBoundHintsPerNodeAndDropOldest()
        {
            service = new HintedHandoffService(metrics, 2, () -> 0L);
            service.recordSet("node", "key-1", "value-1", null);
            service.recordSet("node", "key-2", "value-2", null);
            service.recordSet("node", "key-3", "value-3", null);

            assertEquals(2, service.pendingFor("node"));
            assertEquals(1L, metrics.counter("hinted_handoff_dropped_total").get());

            service.replay("node", node);
            assertEquals(List.of("value-2", "value-3"), node.setValues());
        }

        @Test
        void shouldBoundHintPayloadBytesPerNode()
        {
            service = new HintedHandoffService(metrics, 10, 150L);
            service.recordSet("node", "k", "a".repeat(20), null);
            service.recordSet("node", "k", "b".repeat(20), null);

            assertEquals(1, service.pendingFor("node"));
            assertEquals(1L, metrics.counter("hinted_handoff_dropped_total").get());

            service.replay("node", node);
            assertEquals(List.of("b".repeat(20)), node.setValues());
        }
    }

    @Nested
    class ReplayOperations
    {
        /**
         * Verifies that successful hints are replayed and removed from the queue.
         */
        @Test
        void shouldCleanUpSuccessfulHintsOnReplay()
        {
            // Given
            service.recordSet("node", "key", "value", Duration.ofSeconds(1));
            service.recordDelete("node", "key");
            service.recordCas("node", "key", "value", 1L, Duration.ofSeconds(1));
            
            // When
            service.replay("node", node);
            
            // Then
            assertEquals(1, node.setCallCount(), "Node should receive 1 set call");
            assertEquals(1, node.deleteCallCount(), "Node should receive 1 delete call");
            assertEquals(1, node.casCallCount(), "Node should receive 1 CAS call");
            assertEquals(0, service.pendingFor("node"), "Pending hints should be cleared after successful replay");
            assertEquals(3L, metrics.counter("hinted_handoff_replayed_total").get(), "Replayed hints counter should reflect 3 successful replays");
        }

        /**
         * Verifies that a failed hint remains in the queue and the failure metric is incremented.
         */
        @Test
        void shouldLeaveHintInQueueOnReplayFailure()
        {
            // Given
            service.recordSet("node", "key", "value", Duration.ZERO);
            node.throwNextSet();
            
            // When
            service.replay("node", node);
            
            // Then
            assertEquals(1, node.setCallCount(), "Node should receive 1 set call despite failure");
            assertEquals(1, service.pendingFor("node"), "Hint should remain in the pending queue due to failure");
            assertEquals(1L, metrics.counter("hinted_handoff_failures_total").get(), "Failure counter should be incremented");
        }

        @Test
        void shouldLeaveHintInQueueWhenSetRequestsRetry()
        {
            service.recordSet("node", "key", "value", Duration.ZERO);
            node.rejectNextSet();

            service.replay("node", node);

            assertEquals(1, service.pendingFor("node"));
            assertEquals(1L, metrics.counter("hinted_handoff_failures_total").get());
        }

        @Test
        void shouldReplayOnlyRemainingTtlInsteadOfExtendingIt()
        {
            AtomicLong now = new AtomicLong(1_000L);
            service = new HintedHandoffService(metrics, 10, now::get);
            service.recordSet("node", "key", "value", Duration.ofSeconds(5));
            now.set(3_000L);

            service.replay("node", node);

            assertEquals(Duration.ofSeconds(3), node.lastSetTtl());
        }
    }

    private static final class FakeNode implements Node<String, String>
    {
        private boolean throwSet;
        private boolean rejectSet;
        private int setCalls;
        private int deleteCalls;
        private int casCalls;
        private Duration lastSetTtl;
        private final List<String> setValues = new ArrayList<>();

        void throwNextSet()
        {
            this.throwSet = true;
        }

        void rejectNextSet()
        {
            this.rejectSet = true;
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

        Duration lastSetTtl()
        {
            return lastSetTtl;
        }

        List<String> setValues()
        {
            return setValues;
        }

        @Override
        public boolean set(String key, String value, Duration ttl)
        {
            setCalls++;
            lastSetTtl = ttl;
            if (throwSet)
            {
                throwSet = false;
                throw new RuntimeException("set-fail");
            }
            if (rejectSet)
            {
                rejectSet = false;
                return false;
            }
            setValues.add(value);
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
