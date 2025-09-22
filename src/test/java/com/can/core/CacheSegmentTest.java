package com.can.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CacheSegmentTest
{
    private CacheSegment<String> segment;
    private TestEvictionPolicy policy;
    private List<String> removals;

    @BeforeEach
    void setup()
    {
        policy = new TestEvictionPolicy();
        removals = new ArrayList<>();
        segment = new CacheSegment<>(2, policy, removals::add);
    }

    @Nested
    class PutOperations
    {
        @Test
        void put_stores_value_when_policy_admits()
        {
            CacheValue value = new CacheValue(new byte[]{1}, 0);
            assertTrue(segment.put("a", value));
            assertSame(value, segment.get("a"));
            assertEquals(List.of("a"), policy.recordedKeys);
        }

        @Test
        void put_rejects_when_policy_disallows()
        {
            policy.decision = EvictionPolicy.AdmissionDecision.reject();
            assertFalse(segment.put("a", new CacheValue(new byte[]{1}, 0)));
            assertNull(segment.get("a"));
            assertTrue(policy.removedKeys.isEmpty());
        }

        @Test
        void put_evicts_victim_and_notifies()
        {
            segment.put("victim", new CacheValue(new byte[]{1}, 0));
            policy.decision = EvictionPolicy.AdmissionDecision.admit("victim");
            CacheValue replacement = new CacheValue(new byte[]{2}, 0);

            assertTrue(segment.put("new", replacement));
            assertSame(replacement, segment.get("new"));
            assertEquals(List.of("victim"), policy.removedKeys);
            assertEquals(List.of("victim"), removals);
        }

        @Test
        void put_force_removes_oldest_entry()
        {
            segment.put("a", new CacheValue(new byte[]{1}, 0));
            segment.put("b", new CacheValue(new byte[]{2}, 0));

            CacheValue forced = new CacheValue(new byte[]{3}, 0);
            assertTrue(segment.putForce("c", forced));
            assertNull(segment.get("a"));
            assertSame(forced, segment.get("c"));
            assertTrue(policy.removedKeys.contains("a"));
            assertTrue(removals.contains("a"));
        }
    }

    @Nested
    class RetrievalOperations
    {
        @Test
        void get_records_access_when_value_present()
        {
            CacheValue value = new CacheValue(new byte[]{1}, 0);
            segment.put("a", value);
            assertSame(value, segment.get("a"));
            assertTrue(policy.recordedKeys.contains("a"));
        }

        @Test
        void remove_if_matches_honours_expire_at()
        {
            CacheValue value = new CacheValue(new byte[]{1}, 123L);
            segment.put("a", value);
            assertTrue(segment.removeIfMatches("a", 123L));
            assertFalse(segment.removeIfMatches("a", 123L));
            assertEquals(List.of("a"), policy.removedKeys);
            assertEquals(List.of("a"), removals);
        }
    }

    @Nested
    class CasOperations
    {
        @Test
        void compare_and_swap_updates_when_successful()
        {
            CacheValue initial = new CacheValue(new byte[]{1}, 0);
            segment.put("a", initial);

            CacheSegment.CasResult result = segment.compareAndSwap("a", existing ->
                    CacheSegment.CasDecision.success(new CacheValue(new byte[]{2}, 5L)));

            assertTrue(result.success());
            CacheValue stored = segment.get("a");
            assertNotNull(stored);
            assertArrayEquals(new byte[]{2}, stored.value);
            assertEquals(5L, stored.expireAtMillis);
            assertTrue(policy.recordedKeys.contains("a"));
        }

        @Test
        void compare_and_swap_expired_entry_removes_and_notifies()
        {
            CacheValue initial = new CacheValue(new byte[]{1}, 0);
            segment.put("a", initial);

            CacheSegment.CasResult result = segment.compareAndSwap("a", existing -> CacheSegment.CasDecision.expired());

            assertFalse(result.success());
            assertNull(segment.get("a"));
            assertEquals(List.of("a"), policy.removedKeys);
            assertEquals(List.of("a"), removals);
        }

        @Test
        void compare_and_swap_returns_failure_when_decision_null()
        {
            CacheValue initial = new CacheValue(new byte[]{1}, 0);
            segment.put("a", initial);

            CacheSegment.CasResult result = segment.compareAndSwap("a", existing -> null);

            assertFalse(result.success());
            assertNull(result.newValue());
            assertSame(initial, segment.get("a"));
        }
    }

    @Nested
    class MaintenanceOperations
    {
        @Test
        void clear_invokes_policy_removals()
        {
            segment.put("a", new CacheValue(new byte[]{1}, 0));
            segment.put("b", new CacheValue(new byte[]{2}, 0));

            segment.clear();

            assertEquals(List.of("a", "b"), policy.removedKeys);
            assertTrue(removals.isEmpty());
            assertEquals(0, segment.size());
        }

        @Test
        void for_each_iterates_snapshot()
        {
            segment.put("a", new CacheValue(new byte[]{1}, 0));
            segment.put("b", new CacheValue(new byte[]{2}, 0));

            List<String> seen = new ArrayList<>();
            segment.forEach((key, value) -> {
                seen.add(key + value.value[0]);
                segment.put("c", new CacheValue(new byte[]{3}, 0));
            });

            assertEquals(List.of("a1", "b2"), seen);
            assertNotNull(segment.get("c"));
        }
    }

    private static final class TestEvictionPolicy implements EvictionPolicy<String>
    {
        EvictionPolicy.AdmissionDecision<String> decision = EvictionPolicy.AdmissionDecision.admit();
        final List<String> recordedKeys = new ArrayList<>();
        final List<String> removedKeys = new ArrayList<>();

        @Override
        public void recordAccess(String key)
        {
            recordedKeys.add(key);
        }

        @Override
        public AdmissionDecision<String> admit(String key, LinkedHashMap<String, CacheValue> map, int capacity)
        {
            return decision;
        }

        @Override
        public void onRemove(String key)
        {
            removedKeys.add(key);
        }
    }
}
