package com.cancache.agent.core;

import com.cancache.agent.core.model.CacheValue;
import com.cancache.agent.core.model.CasDecision;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CacheSegmentTest
{
    private CacheSegment<String> segment;
    private FakePolicy<String> policy;
    private List<String> removals;

    @BeforeEach
    void setup()
    {
        policy = new FakePolicy<>();
        removals = new ArrayList<>();
        segment = new CacheSegment<>(2, policy, removals::add);
    }

    @Nested
    class PutOperations
    {
        /**
         * Verifies that a new entry is added when the policy admits it.
         */
        @Test
        void shouldAdmitAndAddEntryWhenPolicyAllows()
        {
            // Given / When
            boolean result = segment.put("a", value("1"));
            
            // Then
            assertTrue(result, "Put should succeed when policy allows");
            assertEquals(1, segment.size(), "Segment size should be 1");
            assertEquals(List.of("a"), policy.accesses(), "Policy should record access for the admitted key");
        }

        /**
         * Shows that a put call returns false when the policy rejects the entry.
         */
        @Test
        void shouldReturnFalseWhenPolicyRejectsPut()
        {
            // Given
            policy.rejectNext();
            
            // When
            boolean result = segment.put("b", value("2"));
            
            // Then
            assertFalse(result, "Put should fail when policy rejects");
            assertEquals(0, segment.size(), "Segment size should remain 0");
        }

        /**
         * Proves that rewriting an existing key updates its value.
         */
        @Test
        void shouldUpdateValueWhenPuttingExistingKey()
        {
            // Given
            assertTrue(segment.put("a", value("1")));
            
            // When
            assertTrue(segment.put("a", value("2")));
            
            // Then
            assertEquals("2", text(segment.get("a")), "Value should be updated to '2'");
        }

        /**
         * Verifies that the listener is notified when a victim key is evicted.
         */
        @Test
        void shouldNotifyListenerWhenVictimEvictedOnPut()
        {
            // Given
            assertTrue(segment.put("a", value("1")));
            assertTrue(segment.put("b", value("2")));
            policy.evictNext("a");
            
            // When
            assertTrue(segment.put("c", value("3")));
            
            // Then
            assertEquals(List.of("a"), removals, "Listener should be notified of eviction for 'a'");
            assertTrue(policy.removed().contains("a"), "Policy should record removal of 'a'");
        }

        /**
         * Shows that a forced put evicts the oldest entry when capacity is full.
         */
        @Test
        void shouldEvictOldestWhenForcedPutAtFullCapacity()
        {
            // Given
            assertTrue(segment.put("a", value("1")));
            assertTrue(segment.put("b", value("2")));
            
            // When
            assertTrue(segment.putForce("c", value("3")));
            
            // Then
            assertNull(segment.get("a"), "Oldest entry 'a' should be forcefully evicted");
            assertTrue(removals.contains("a"), "Evicted entry 'a' should trigger listener notification");
        }
    }

    @Nested
    class RemoveOperations
    {
        /**
         * Verifies that remove returns the value and notifies the policy.
         */
        @Test
        void shouldReturnValueAndNotifyPolicyOnRemove()
        {
            // Given
            assertTrue(segment.put("a", value("1")));
            
            // When
            CacheValue removed = segment.remove("a");
            
            // Then
            assertNotNull(removed, "Removed value should not be null");
            assertEquals(List.of("a"), policy.removed(), "Policy should be notified of the removal");
        }

        /**
         * Proves that conditional removal succeeds if the expireAt timestamp matches.
         */
        @Test
        void shouldRemoveIfTimestampMatchesOnRemoveIfMatches()
        {
            // Given
            assertTrue(segment.put("a", new CacheValue("v".getBytes(StandardCharsets.UTF_8), 123L)));
            
            // When
            boolean failedRemove = segment.removeIfMatches("a", 999L);
            boolean successfulRemove = segment.removeIfMatches("a", 123L);
            
            // Then
            assertFalse(failedRemove, "Remove should fail if timestamps mismatch");
            assertTrue(successfulRemove, "Remove should succeed if timestamps match");
            assertTrue(removals.contains("a"), "Removal should notify listener");
        }

        /**
         * Verifies that the clear call removes all entries.
         */
        @Test
        void shouldRemoveAllEntriesOnClear()
        {
            // Given
            assertTrue(segment.put("a", value("1")));
            assertTrue(segment.put("b", value("2")));
            
            // When
            segment.clear();
            
            // Then
            assertEquals(0, segment.size(), "Size should be 0 after clear");
            assertTrue(policy.removed().containsAll(List.of("a", "b")), "Policy should be notified of all removals");
        }
    }

    @Nested
    class CasOperations
    {
        /**
         * Shows that the new value is written if the CAS decision is successful.
         */
        @Test
        void shouldWriteNewValueOnCasSuccess()
        {
            // Given
            assertTrue(segment.put("a", value("old")));
            
            // When
            var result = segment.compareAndSwap("a", existing -> CasDecision.success(value("new")));
            
            // Then
            assertTrue(result.success(), "CAS should succeed");
            assertEquals("new", text(segment.get("a")), "Value should be updated to 'new'");
            assertTrue(policy.accesses().contains("a"), "Policy should register an access");
        }

        /**
         * Verifies that the listener is called when a CAS decision results in deleting the current entry.
         */
        @Test
        void shouldNotifyListenerWhenCasDeletesEntry()
        {
            // Given
            assertTrue(segment.put("a", value("old")));
            
            // When
            var result = segment.compareAndSwap("a", existing -> CasDecision.expired());
            
            // Then
            assertFalse(result.success(), "CAS should fail due to expiration");
            assertNull(segment.get("a"), "Value should be removed after expiration CAS");
            assertTrue(removals.contains("a"), "Listener should be notified of removal");
        }
    }

    @Nested
    class OtherOperations
    {
        /**
         * Verifies that a forEach iteration operates safely over a snapshot.
         */
        @Test
        void shouldOperateSafelyOverSnapshotOnForEach()
        {
            // Given
            assertTrue(segment.put("a", value("1")));
            assertTrue(segment.put("b", value("2")));
            List<String> keys = new ArrayList<>();
            
            // When
            segment.forEach((key, value) -> {
                keys.add(key);
                segment.put("c", value("3"));
            });
            
            // Then
            assertEquals(List.of("a", "b"), keys, "Iteration should only cover original entries");
            assertEquals(3, segment.size(), "Segment size should grow despite concurrent modification via snapshot iteration");
        }
    }

    private static CacheValue value(String text)
    {
        return new CacheValue(text.getBytes(StandardCharsets.UTF_8), 0L);
    }

    private static String text(CacheValue value)
    {
        return new String(value.value(), StandardCharsets.UTF_8);
    }

    private static final class FakePolicy<K> implements EvictionPolicy<K>
    {
        private final List<K> accesses = new ArrayList<>();
        private final List<K> removed = new ArrayList<>();
        private EvictionPolicy.AdmissionDecision<K> nextDecision = EvictionPolicy.AdmissionDecision.admit();

        @Override
        public void recordAccess(K key)
        {
            accesses.add(key);
        }

        @Override
        public AdmissionDecision<K> admit(K key, LinkedHashMap<K, CacheValue> map, int capacity)
        {
            AdmissionDecision<K> decision = nextDecision;
            nextDecision = EvictionPolicy.AdmissionDecision.admit();
            return decision;
        }

        @Override
        public void onRemove(K key)
        {
            removed.add(key);
        }

        void rejectNext()
        {
            nextDecision = EvictionPolicy.AdmissionDecision.reject();
        }

        void evictNext(K key)
        {
            nextDecision = EvictionPolicy.AdmissionDecision.admit(key);
        }

        List<K> accesses()
        {
            return accesses;
        }

        List<K> removed()
        {
            return removed;
        }
    }
}
