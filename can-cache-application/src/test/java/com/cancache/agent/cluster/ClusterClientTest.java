package com.cancache.agent.cluster;

import com.cancache.agent.codec.StringCodec;
import com.cancache.agent.constants.NodeProtocol;
import com.cancache.agent.metric.MetricsRegistry;
import org.junit.jupiter.api.AfterEach;
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

    @AfterEach
    void cleanup()
    {
        client.close();
    }

    @Nested
    class SetOperations
    {
        /**
         * Verifies that the set call returns false when the hash ring is empty.
         */
        @Test
        void shouldReturnFalseOnEmptyRingWhenSetIsCalled()
        {
            // Given
            ConsistentHashRing<Node<String, String>> emptyRing = new ConsistentHashRing<>(new ControlledHash(), 1);
            try (ClusterClient emptyClient = new ClusterClient(emptyRing, 3, StringCodec.UTF8, handoff)) {
                assertFalse(emptyClient.set("clientKey", "value", null),
                        "Set should return false if no nodes are available in the ring");
            }
        }

        /**
         * A semantic rejection is not retried as a connectivity failure.
         */
        @Test
        void shouldReturnTrueWhenQuorumReachedOnSet()
        {
            // Given
            replica1.failNextSet();

            // When
            boolean result = client.set("clientKey", "value", Duration.ofSeconds(1));

            // Then
            assertTrue(result, "Set should return true when a quorum of nodes succeeds");
            assertEquals(0, handoff.pendingFor(replica1.id()), "Rejected writes must not consume handoff memory");
            assertEquals(0, handoff.pendingFor(replica2.id()), "No messages should be queued for the successful replica2");
            assertEquals(0, handoff.pendingFor(leader.id()), "No messages should be queued for the successful leader");
        }

        @Test
        void shouldNotQueueLeaderHintWhenLeaderRejectsButReplicaQuorumSucceeds()
        {
            leader.failNextSet();

            assertTrue(client.set("clientKey", "value", Duration.ofSeconds(1)));
            assertEquals(0, handoff.pendingFor(leader.id()),
                    "A semantic rejection must not be replayed indefinitely");
        }

        @Test
        void shouldNotShrinkConfiguredQuorumWhenReplicasAreMissing()
        {
            ConsistentHashRing<Node<String, String>> partialRing = new ConsistentHashRing<>(new ControlledHash(), 1);
            partialRing.addNode(leader, bytes("leader"));
            try (ClusterClient partialClient = new ClusterClient(partialRing, 3, StringCodec.UTF8, handoff)) {
                assertFalse(partialClient.set("clientKey", "value", null),
                        "RF=3 must not silently become quorum=1 during a partition");
            }
        }

        /**
         * Verifies that an exception is thrown when the leader node fails and a quorum cannot be met.
         */
        @Test
        void shouldThrowExceptionWhenLeaderFailsAndNoQuorumOnSet()
        {
            // Given
            leader.throwNextSet();
            replica1.failNextSet();
            replica2.failNextSet();

            // When
            RuntimeException ex = assertThrows(RuntimeException.class, () -> client.set("clientKey", "value", null), "Should throw an exception if set quorum fails completely");

            // Then
            assertTrue(ex.getMessage().contains("set"), "Exception message should indicate a set failure");
            assertEquals(1, handoff.pendingFor(leader.id()), "Hinted handoff should queue a message for the failed leader");
            assertEquals(0, handoff.pendingFor(replica1.id()), "Rejected writes must not be retried as outages");
            assertEquals(0, handoff.pendingFor(replica2.id()), "Rejected writes must not be retried as outages");
        }
    }

    @Nested
    class ReadOperations
    {
        /**
         * Verifies that the value is returned from the first successful replica.
         */
        @Test
        void shouldReturnFirstSuccessfulReplicaValueOnGet()
        {
            // Given
            replica1.preset("value");

            // When
            String result = client.get("clientKey");

            // Then
            assertEquals("value", result, "Get should return the value from the successful replica");
        }

        /**
         * Demonstrates that null is returned if no replicas yield a value.
         */
        @Test
        void shouldReturnNullWhenAllReplicasEmptyOnGet()
        {
            // Given (Replicas are empty by default)

            // When
            String result = client.get("clientKey");

            // Then
            assertNull(result, "Get should return null if the value doesn't exist on any replica");
        }

        /**
         * Verifies that the process continues to the next node and succeeds if the first node throws an exception.
         */
        @Test
        void shouldContinueToNextNodeOnExceptionDuringGet()
        {
            // Given
            leader.throwNextGet();
            replica1.preset("fallback-value");

            // When
            String result = client.get("clientKey");

            // Then
            assertEquals("fallback-value", result, "Get should fall back to the next replica and retrieve the value if the first one throws");
        }
    }

    @Nested
    class DeleteOperations
    {
        /**
         * Verifies that true is returned when two replicas successfully complete the deletion.
         */
        @Test
        void shouldReturnTrueWithQuorumOnDelete()
        {
            // Given
            replica2.failNextDelete();

            // When
            boolean result = client.delete("clientKey");

            // Then
            assertTrue(result, "Delete should succeed when quorum is met");
            assertEquals(0, handoff.pendingFor(replica2.id()), "An already-absent delete needs no handoff hint");
        }

        /**
         * Demonstrates that false is returned without retaining no-op delete hints.
         */
        @Test
        void shouldReturnFalseWithoutQuorumOnDelete()
        {
            // Given
            leader.failNextDelete();
            replica1.failNextDelete();

            // When
            boolean result = client.delete("clientKey");

            // Then
            assertFalse(result, "Delete should fail when quorum is not met");
            assertEquals(0, handoff.pendingFor(replica1.id()), "Absent-key deletes must not consume handoff memory");
            assertEquals(0, handoff.pendingFor(replica2.id()), "No handoff hint should be saved for successful replica2");
        }
    }

    @Nested
    class CasOperations
    {
        /**
         * Verifies that the CAS operation returns true when the quorum is met.
         */
        @Test
        void shouldReturnTrueWithQuorumOnCompareAndSwap()
        {
            // Given
            replica2.failNextCas();

            // When
            boolean result = client.compareAndSwap("clientKey", "v", 1L, Duration.ofSeconds(1));

            // Then
            assertTrue(result, "CAS should succeed when quorum is met");
            assertEquals(0, handoff.pendingFor(replica2.id()), "Rejected CAS operations are not replayable hints");
        }

        /**
         * Demonstrates that an exception is thrown when the leader fails and a quorum cannot be met.
         */
        @Test
        void shouldThrowWhenLeaderFailsOnCompareAndSwap()
        {
            // Given
            leader.throwNextCas();
            replica1.failNextCas();
            replica2.failNextCas();

            // When
            RuntimeException ex = assertThrows(RuntimeException.class, () -> client.compareAndSwap("clientKey", "v", 1L, null), "Exception expected when CAS quorum fails");

            // Then
            assertTrue(ex.getMessage().contains("cas"), "Exception message should indicate a CAS failure");
            assertEquals(1, handoff.pendingFor(leader.id()), "Connectivity failures must retain a repair hint");
        }

        @Test
        void shouldUseTheReservedCasTokenForAtomicAdd()
        {
            assertTrue(client.add("clientKey", "new", null));
            assertEquals(NodeProtocol.CAS_EXPECT_ABSENT, leader.lastExpectedCas);
            assertEquals(NodeProtocol.CAS_EXPECT_ABSENT, replica1.lastExpectedCas);
            assertEquals(NodeProtocol.CAS_EXPECT_ABSENT, replica2.lastExpectedCas);
        }
    }

    @Nested
    class MaintenanceOperations
    {
        /**
         * Verifies that the clear call is executed across all nodes.
         */
        @Test
        void shouldInvokeClearOnAllNodes()
        {
            // Given / When
            client.clear();

            // Then
            assertEquals(1, leader.clearCalls, "Leader should receive clear call");
            assertEquals(1, replica1.clearCalls, "Replica1 should receive clear call");
            assertEquals(1, replica2.clearCalls, "Replica2 should receive clear call");
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
        private boolean throwGet;
        private String storedValue;
        private int clearCalls;
        private long lastExpectedCas;

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

        void throwNextGet()
        {
            this.throwGet = true;
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
            if (throwGet)
            {
                throwGet = false;
                throw new RuntimeException("get-fail-" + id);
            }
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
            lastExpectedCas = expectedCas;
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
