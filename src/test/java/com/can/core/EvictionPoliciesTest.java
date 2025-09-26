package com.can.core;

import com.can.core.model.CacheValue;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;

class EvictionPoliciesTest
{
    @Nested
    class TypeResolution
    {
        // Bu test yapılandırma değeri boş olduğunda LRU politikasının seçildiğini doğrular.
        @Test
        void from_config_returns_lru_for_blank_value()
        {
            assertEquals(EvictionPolicyType.LRU, EvictionPolicyType.fromConfig(null));
            assertEquals(EvictionPolicyType.LRU, EvictionPolicyType.fromConfig(" "));
        }

        // Bu test farklı yazımlarla verilen TinyLFU değerinin doğru çözümlendiğini gösterir.
        @Test
        void from_config_normalizes_tiny_lfu()
        {
            assertEquals(EvictionPolicyType.TINY_LFU, EvictionPolicyType.fromConfig("tiny-lfu"));
            assertEquals(EvictionPolicyType.TINY_LFU, EvictionPolicyType.fromConfig("Tiny_Lfu"));
        }

        // Bu test bilinmeyen politika değerinde istisna fırlatıldığını doğrular.
        @Test
        void from_config_throws_for_unknown_value()
        {
            assertThrows(IllegalArgumentException.class, () -> EvictionPolicyType.fromConfig("unknown"));
        }
    }

    @Nested
    class LruBehavior
    {
        // Bu test kapasite dolmadığında yeni anahtarın doğrudan kabul edildiğini gösterir.
        @Test
        void lru_admits_candidate_when_capacity_available()
        {
            LruEvictionPolicy<String> policy = new LruEvictionPolicy<>();
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            var decision = policy.admit("candidate", map, 2);
            assertTrue(decision.shouldAdmit());
            assertNull(decision.evictKey());
        }

        // Bu test kapasite dolduğunda en eski girdinin kurban seçildiğini doğrular.
        @Test
        void lru_evicts_oldest_when_full()
        {
            LruEvictionPolicy<String> policy = new LruEvictionPolicy<>();
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            map.put("old", new CacheValue(new byte[]{1}, 0L));
            map.put("young", new CacheValue(new byte[]{2}, 0L));
            var decision = policy.admit("candidate", map, 2);
            assertTrue(decision.shouldAdmit());
            assertEquals("old", decision.evictKey());
        }
    }

    @Nested
    class TinyLfuBehavior
    {
        // Bu test boş kapasitede TinyLFU'nun adayı kabul ettiğini doğrular.
        @Test
        void tiny_lfu_admits_candidate_with_free_capacity()
        {
            TinyLfuEvictionPolicy<String> policy = new TinyLfuEvictionPolicy<>(2);
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            var decision = policy.admit("candidate", map, 2);
            assertTrue(decision.shouldAdmit());
            assertNull(decision.evictKey());
        }

        // Bu test adayın frekansı kurbandan yüksek olduğunda kabul edildiğini gösterir.
        @Test
        void tiny_lfu_admits_candidate_with_higher_frequency()
        {
            TinyLfuEvictionPolicy<String> policy = new TinyLfuEvictionPolicy<>(1);
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            map.put("victim", new CacheValue(new byte[]{1}, 0L));
            policy.recordAccess("victim");
            policy.recordAccess("candidate");
            policy.recordAccess("candidate");
            policy.recordAccess("candidate");
            var decision = policy.admit("candidate", map, 1);
            assertTrue(decision.shouldAdmit());
            assertEquals("victim", decision.evictKey());
        }

        // Bu test adayın frekansı düşük olduğunda reddedildiğini doğrular.
        @Test
        void tiny_lfu_rejects_low_frequency_candidate()
        {
            TinyLfuEvictionPolicy<String> policy = new TinyLfuEvictionPolicy<>(1);
            LinkedHashMap<String, CacheValue> map = new LinkedHashMap<>();
            map.put("victim", new CacheValue(new byte[]{1}, 0L));
            policy.recordAccess("victim");
            policy.recordAccess("victim");
            policy.recordAccess("candidate");
            var decision = policy.admit("candidate", map, 1);
            assertFalse(decision.shouldAdmit());
        }
    }
}
