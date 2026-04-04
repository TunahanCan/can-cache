package com.cancache.agent.cluster;

import com.cancache.agent.metric.MetricsRegistry;
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
