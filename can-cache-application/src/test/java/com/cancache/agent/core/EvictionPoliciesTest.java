package com.cancache.agent.core;

import com.cancache.agent.core.model.CacheValue;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;

class EvictionPoliciesTest
{
    @Nested
    class TypeResolution
    {
        /**
         * Verifies that the LRU policy is selected when the configuration value is blank.
         */
        @Test
        void shouldReturnLruForBlankConfigValue()
        {
            // Given / When / Then
            assertEquals(EvictionPolicyType.LRU, EvictionPolicyType.fromConfig(null), "Null config should default to LRU");
            assertEquals(EvictionPolicyType.LRU, EvictionPolicyType.fromConfig(" "), "Blank config should default to LRU");
        }

        /**
         * Shows that variations in case for 'tiny-lfu' are normalized and parsed correctly.
         */
        @Test
        void shouldNormalizeTinyLfuConfigValue()
        {
            // Given / When / Then
            assertEquals(EvictionPolicyType.TINY_LFU, EvictionPolicyType.fromConfig("tiny-lfu"), "Lowercase tiny-lfu should parse");
            assertEquals(EvictionPolicyType.TINY_LFU, EvictionPolicyType.fromConfig("Tiny_Lfu"), "Mixed case Tiny_Lfu should parse");
        }

        /**
         * Verifies that an exception is thrown for an unknown policy value.
         */
        @Test
        void shouldThrowExceptionForUnknownConfigValue()
        {
            // Given / When / Then
            assertThrows(IllegalArgumentException.class, () -> EvictionPolicyType.fromConfig("unknown"), "Unknown value should trigger an exception");
        }
    }

    @Nested
    class LruBehavior
    {
        /**
         * Shows that a new key is admitted directly when capacity is available.
         */
        @Test
        void shouldAdmitCandidateWhenCapacityAvailableInLru()
        {
            // Given
            LruEvictionPolicy<String> policy = new LruEvictionPolicy<>();
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            
            // When
            var decision = policy.admit("candidate", map, false);
            
            // Then
            assertTrue(decision.shouldAdmit(), "Candidate should be admitted");
            assertNull(decision.evictKey(), "No eviction key should be provided");
        }

        /**
         * Verifies that the oldest entry is chosen as a victim when capacity is full.
         */
        @Test
        void shouldEvictOldestWhenFullInLru()
        {
            // Given
            LruEvictionPolicy<String> policy = new LruEvictionPolicy<>();
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            map.put("old", new CacheValue(new byte[]{1}, 0L));
            map.put("young", new CacheValue(new byte[]{2}, 0L));
            
            // When
            var decision = policy.admit("candidate", map, true);
            
            // Then
            assertTrue(decision.shouldAdmit(), "Candidate should be admitted");
            assertEquals("old", decision.evictKey(), "Oldest key should be chosen for eviction");
        }
    }

    @Nested
    class TinyLfuBehavior
    {
        /**
         * Verifies that TinyLFU admits a candidate when there is free capacity.
         */
        @Test
        void shouldAdmitCandidateWithFreeCapacityInTinyLfu()
        {
            // Given
            TinyLfuEvictionPolicy<String> policy = new TinyLfuEvictionPolicy<>(2);
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            
            // When
            var decision = policy.admit("candidate", map, false);
            
            // Then
            assertTrue(decision.shouldAdmit(), "Candidate should be admitted");
            assertNull(decision.evictKey(), "No eviction key should be provided");
        }

        /**
         * Shows that a candidate is admitted and a victim is evicted when the candidate has a higher frequency.
         */
        @Test
        void shouldAdmitCandidateWithHigherFrequencyInTinyLfu()
        {
            // Given
            TinyLfuEvictionPolicy<String> policy = new TinyLfuEvictionPolicy<>(1);
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            map.put("victim", new CacheValue(new byte[]{1}, 0L));
            policy.recordAccess("victim");
            policy.recordAccess("candidate");
            policy.recordAccess("candidate");
            policy.recordAccess("candidate"); // Candidate has higher frequency
            
            // When
            var decision = policy.admit("candidate", map, true);
            
            // Then
            assertTrue(decision.shouldAdmit(), "Candidate with higher frequency should be admitted");
            assertEquals("victim", decision.evictKey(), "Victim with lower frequency should be evicted");
        }

        /**
         * Verifies that a candidate is rejected if its frequency is lower than the victim's.
         */
        @Test
        void shouldRejectCandidateWithLowerFrequencyInTinyLfu()
        {
            // Given
            TinyLfuEvictionPolicy<String> policy = new TinyLfuEvictionPolicy<>(1);
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            map.put("victim", new CacheValue(new byte[]{1}, 0L));
            policy.recordAccess("victim");
            policy.recordAccess("victim"); // Victim has higher frequency
            policy.recordAccess("candidate");
            
            // When
            var decision = policy.admit("candidate", map, true);
            
            // Then
            assertFalse(decision.shouldAdmit(), "Candidate with lower frequency should be rejected");
        }
    }
}
