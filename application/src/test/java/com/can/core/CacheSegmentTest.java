package com.can.core;

import com.can.core.model.CacheValue;
import com.can.core.model.CasDecision;
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
        // Bu test politika izin verdiğinde yeni girdinin eklendiğini doğrular.
        @Test
        void put_succeeds_when_key_is_admitted()
        {
            assertTrue(segment.put("a", value("1")));
            assertEquals(1, segment.size());
            assertEquals(List.of("a"), policy.accesses());
        }

        // Bu test politika reddettiğinde put çağrısının başarısız döndüğünü gösterir.
        @Test
        void put_returns_false_when_policy_rejects()
        {
            policy.rejectNext();
            assertFalse(segment.put("b", value("2")));
            assertEquals(0, segment.size());
        }

        // Bu test mevcut anahtar tekrar yazıldığında değerin güncellendiğini ispatlar.
        @Test
        void put_updates_existing_key()
        {
            assertTrue(segment.put("a", value("1")));
            assertTrue(segment.put("a", value("2")));
            assertEquals("2", text(segment.get("a")));
        }

        // Bu test kurban anahtar belirlendiğinde dinleyicinin bilgilendirildiğini doğrular.
        @Test
        void put_notifies_listener_when_victim_selected()
        {
            assertTrue(segment.put("a", value("1")));
            assertTrue(segment.put("b", value("2")));
            policy.evictNext("a");
            assertTrue(segment.put("c", value("3")));
            assertEquals(List.of("a"), removals);
            assertTrue(policy.removed().contains("a"));
        }

        // Bu test force eklemenin kapasite dolu olsa bile en eski girdiyi attığını gösterir.
        @Test
        void put_force_evicts_oldest_when_full()
        {
            assertTrue(segment.put("a", value("1")));
            assertTrue(segment.put("b", value("2")));
            assertTrue(segment.putForce("c", value("3")));
            assertNull(segment.get("a"));
            assertTrue(removals.contains("a"));
        }
    }

    @Nested
    class RemoveOperations
    {
        // Bu test remove çağrısının değeri döndürüp politikayı bilgilendirdiğini doğrular.
        @Test
        void remove_returns_value_when_present()
        {
            assertTrue(segment.put("a", value("1")));
            CacheValue removed = segment.remove("a");
            assertNotNull(removed);
            assertEquals(List.of("a"), policy.removed());
        }

        // Bu test expireAt eşleştiğinde koşullu silmenin başarılı olduğunu ispatlar.
        @Test
        void remove_if_matches_deletes_when_timestamp_matches()
        {
            assertTrue(segment.put("a", new CacheValue("v".getBytes(StandardCharsets.UTF_8), 123L)));
            assertFalse(segment.removeIfMatches("a", 999L));
            assertTrue(segment.removeIfMatches("a", 123L));
            assertTrue(removals.contains("a"));
        }

        // Bu test clear çağrısının tüm girdileri temizlediğini doğrular.
        @Test
        void clear_removes_all_entries()
        {
            assertTrue(segment.put("a", value("1")));
            assertTrue(segment.put("b", value("2")));
            segment.clear();
            assertEquals(0, segment.size());
            assertTrue(policy.removed().containsAll(List.of("a", "b")));
        }
    }

    @Nested
    class CasOperations
    {
        // Bu test CAS kararı başarı olduğunda yeni değerin yazıldığını gösterir.
        @Test
        void compare_and_swap_writes_new_value_on_success()
        {
            assertTrue(segment.put("a", value("old")));
            var result = segment.compareAndSwap("a", existing -> CasDecision.success(value("new")));
            assertTrue(result.success());
            assertEquals("new", text(segment.get("a")));
            assertTrue(policy.accesses().contains("a"));
        }

        // Bu test CAS kararı mevcut girdiyi kaldırdığında dinleyicinin çağrıldığını doğrular.
        @Test
        void compare_and_swap_notifies_listener_on_delete()
        {
            assertTrue(segment.put("a", value("old")));
            var result = segment.compareAndSwap("a", existing -> CasDecision.expired());
            assertFalse(result.success());
            assertNull(segment.get("a"));
            assertTrue(removals.contains("a"));
        }
    }

    @Nested
    class OtherOperations
    {
        // Bu test forEach çağrısının snapshot üzerinden güvenle çalıştığını doğrular.
        @Test
        void for_each_continues_operating_on_snapshot()
        {
            assertTrue(segment.put("a", value("1")));
            assertTrue(segment.put("b", value("2")));
            List<String> keys = new ArrayList<>();
            segment.forEach((key, value) -> {
                keys.add(key);
                segment.put("c", value("3"));
            });
            assertEquals(List.of("a", "b"), keys);
            assertEquals(3, segment.size());
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
