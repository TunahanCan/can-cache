package com.can.core;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CacheSegment sınıfı segment bazlı veri tutar; kilitli LinkedHashMap kullanır ve EvictionPolicy ile bütünleşir.
 * Aşağıdaki testler politikanın etkileşimi, zorunlu yazma ve snapshot davranışını detaylıca dokümante eder.
 */
class CacheSegmentDetailedTest {

    private static CacheValue value(int marker) {
        return new CacheValue(new byte[]{(byte) marker}, 0L);
    }

    @Nested
    class PutOperations {
        /**
         * Politika yeni girişe izin vermezse CacheSegment put false döner ve mevcut veri korunur.
         * Bu sayede TinyLFU gibi politikalar soğuk anahtarları engelleyebilir.
         */
        @Test
        void rejectsInsertionWhenPolicyDisagrees() {
            RecordingPolicy<String> policy = new RecordingPolicy<>();
            CacheSegment<String> segment = new CacheSegment<>(1, policy);

            policy.nextDecision(EvictionPolicy.AdmissionDecision.admit());
            assertTrue(segment.put("hot", value(1)));

            policy.nextDecision(EvictionPolicy.AdmissionDecision.reject());
            assertFalse(segment.put("cold", value(2)), "Politika reddettiğinde segment yazmamalı");
            assertNotNull(segment.get("hot"));
            assertNull(segment.get("cold"));
            assertEquals(2, policy.accessCount.get(), "Hem mevcut hem aday erişimler kaydedildi");
        }

        /**
         * Politika bir kurban seçerse segment ilgili anahtarı map'ten çıkartıp yeni girdiyi ekler.
         * onRemove çağrısı ile politika kendi iç istatistiklerini temizleyebilir.
         */
        @Test
        void evictsVictimSelectedByPolicy() {
            RecordingPolicy<String> policy = new RecordingPolicy<>();
            CacheSegment<String> segment = new CacheSegment<>(1, policy);

            policy.nextDecision(EvictionPolicy.AdmissionDecision.admit());
            assertTrue(segment.put("hot", value(1)));

            policy.nextDecision(EvictionPolicy.AdmissionDecision.admit("hot"));
            assertTrue(segment.put("fresh", value(2)));
            assertNull(segment.get("hot"), "Kurban kaldırılmış olmalı");
            assertNotNull(segment.get("fresh"));
            assertEquals(List.of("hot"), policy.removedKeys, "Politika onRemove ile haberdar edilir");
        }
    }

    @Nested
    class ForcedInsertions {
        /**
         * putForce kapasite dolu olsa bile sıradaki en eski girdiyi çıkarır; admission kontrolü yapılmaz.
         * Snapshot yükleme gibi zorunlu durumlarda kullanılır, bu yüzden map boyutu korunurken veri yer değiştirir.
         */
        @Test
        void putForceOverridesCapacity() {
            RecordingPolicy<String> policy = new RecordingPolicy<>();
            CacheSegment<String> segment = new CacheSegment<>(1, policy);

            policy.nextDecision(EvictionPolicy.AdmissionDecision.admit());
            segment.put("old", value(1));

            assertTrue(segment.putForce("new", value(2)));
            assertNull(segment.get("old"));
            assertNotNull(segment.get("new"));
            assertEquals(List.of("old"), policy.removedKeys, "Zorunlu kaldırmada da politika bilgilendirilir");
        }

        /**
         * remove çağrısı var olan kaydı döndürür ve politikanın onRemove kancası tetiklenir.
         * Bu mekanizma TTL temizleyicisi gibi bileşenler tarafından kullanılır.
         */
        @Test
        void removeNotifiesPolicy() {
            RecordingPolicy<String> policy = new RecordingPolicy<>();
            CacheSegment<String> segment = new CacheSegment<>(4, policy);

            policy.nextDecision(EvictionPolicy.AdmissionDecision.admit());
            segment.put("sample", value(7));

            assertNotNull(segment.remove("sample"));
            assertNull(segment.get("sample"));
            assertEquals(List.of("sample"), policy.removedKeys);
        }
    }

    @Nested
    class SnapshotView {
        /**
         * forEach metodu kilitli map'in kopyasını oluşturur ve dışarıya tutarlı bir anlık görüntü verir.
         * Test sırasında callback içinden map değişse bile orijinal iterasyon bozulmamalı.
         */
        @Test
        void forEachUsesSnapshotToAvoidConcurrentMutation() {
            RecordingPolicy<String> policy = new RecordingPolicy<>();
            CacheSegment<String> segment = new CacheSegment<>(4, policy);

            policy.nextDecision(EvictionPolicy.AdmissionDecision.admit());
            segment.put("a", value(1));
            policy.nextDecision(EvictionPolicy.AdmissionDecision.admit());
            segment.put("b", value(2));

            List<String> seen = new ArrayList<>();
            segment.forEach((key, value) -> {
                seen.add(key + value.value[0]);
                segment.remove(key);
            });

            assertEquals(List.of("a1", "b2"), seen, "Snapshot sayesinde iterasyon sırasında değişiklik sorun çıkarmaz");
            assertEquals(0, segment.size(), "Callback sonrası removelar uygulanmış olmalı");
        }
    }

    /** Basit kayıt tutan politika implementasyonu */
    private static final class RecordingPolicy<K> implements EvictionPolicy<K> {
        private final AtomicInteger accessCount = new AtomicInteger();
        private final List<K> removedKeys = new ArrayList<>();
        private AdmissionDecision<K> next = AdmissionDecision.admit();

        void nextDecision(AdmissionDecision<K> decision) {
            this.next = decision;
        }

        @Override
        public void recordAccess(K key) {
            accessCount.incrementAndGet();
        }

        @Override
        public AdmissionDecision<K> admit(K key, java.util.LinkedHashMap<K, CacheValue> map, int capacity) {
            AdmissionDecision<K> current = next;
            next = AdmissionDecision.admit();
            return current;
        }

        @Override
        public void onRemove(K key) {
            removedKeys.add(key);
        }
    }
}
