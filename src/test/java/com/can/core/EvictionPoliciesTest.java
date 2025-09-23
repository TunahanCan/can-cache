package com.can.core;

import com.can.core.model.CacheValue;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;

class EvictionPoliciesTest
{
    @Nested
    class LruPolicy
    {
        private final LruEvictionPolicy<String> policy = new LruEvictionPolicy<>();

        @Test
        void admit_allows_when_capacity_not_reached()
        {
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            map.put("a", new CacheValue(new byte[]{1}, 0));
            EvictionPolicy.AdmissionDecision<String> decision = policy.admit("b", map, 2);
            assertTrue(decision.shouldAdmit());
            assertNull(decision.evictKey());
        }

        @Test
        void admit_evicts_eldest_when_full()
        {
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            map.put("first", new CacheValue(new byte[]{1}, 0));
            map.put("second", new CacheValue(new byte[]{2}, 0));

            EvictionPolicy.AdmissionDecision<String> decision = policy.admit("third", map, 2);
            assertTrue(decision.shouldAdmit());
            assertEquals("first", decision.evictKey());
        }
    }

    @Nested
    class TinyLfuPolicy
    {
        @Test
        void admit_rejects_when_candidate_less_frequent()
        {
            TinyLfuEvictionPolicy<String> policy = new TinyLfuEvictionPolicy<>(1);
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            map.put("victim", new CacheValue(new byte[]{1}, 0));

            for (int i = 0; i < 10; i++) policy.recordAccess("victim");
            policy.recordAccess("candidate");

            EvictionPolicy.AdmissionDecision<String> decision = policy.admit("candidate", map, 1);
            assertFalse(decision.shouldAdmit());
        }

        @Test
        void admit_prefers_more_frequent_candidate()
        {
            TinyLfuEvictionPolicy<String> policy = new TinyLfuEvictionPolicy<>(1);
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            map.put("victim", new CacheValue(new byte[]{1}, 0));

            for (int i = 0; i < 5; i++) policy.recordAccess("victim");
            for (int i = 0; i < 20; i++) policy.recordAccess("candidate");

            EvictionPolicy.AdmissionDecision<String> decision = policy.admit("candidate", map, 1);
            assertTrue(decision.shouldAdmit());
            assertEquals("victim", decision.evictKey());
        }
    }

    @Nested
    class TypeMapping
    {
        @Test
        void from_config_defaults_to_lru()
        {
            assertEquals(EvictionPolicyType.LRU, EvictionPolicyType.fromConfig(null));
            assertEquals(EvictionPolicyType.LRU, EvictionPolicyType.fromConfig(""));
        }

        @Test
        void from_config_accepts_various_formats()
        {
            assertEquals(EvictionPolicyType.TINY_LFU, EvictionPolicyType.fromConfig("tiny-lfu"));
            assertEquals(EvictionPolicyType.LRU, EvictionPolicyType.fromConfig("lru"));
        }

        @Test
        void from_config_rejects_unknown_values()
        {
            assertThrows(IllegalArgumentException.class, () -> EvictionPolicyType.fromConfig("unknown"));
        }
    }
}
