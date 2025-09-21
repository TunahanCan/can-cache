package com.can.core;

import java.util.LinkedHashMap;

/**
 * Bir segmentin hangi anahtarları kabul edeceğini ve kapasite aşıldığında hangi
 * girdilerin tahliye edileceğini tanımlayan politika arayüzüdür. Uygulamalar
 * erişim geçmişi veya frekans istatistiklerinden yararlanarak karar verebilir.
 */
interface EvictionPolicy<K>
{
    /** Verilen anahtarın erişildiğini kaydeder. */
    void recordAccess(K key);

    /**
     * Yeni bir anahtar segment içine eklenmeden önce çağrılarak kabul kurallarını uygular.
     *
     * @param key       kabul edilmek istenen aday anahtar
     * @param map       segmentin erişim sırasına göre tutulan mevcut haritası
     * @param capacity  segment için tanımlı maksimum giriş sayısı
     * @return anahtarın kabul edilip edilmeyeceğini ve gerekirse kurban anahtarı döndürür
     */
    AdmissionDecision<K> admit(K key, LinkedHashMap<K, CacheValue> map, int capacity);

    /** Belirtilen anahtarın segmentten çıkarıldığını bildirir. */
    void onRemove(K key);

    /** {@link #admit(Object, LinkedHashMap, int)} çağrısının sonucunu kapsüller. */
    final class AdmissionDecision<K>
    {
        private static final AdmissionDecision<?> REJECT = new AdmissionDecision<>(false, null);
        private static final AdmissionDecision<?> ADMIT_NO_EVICTION = new AdmissionDecision<>(true, null);

        private final boolean admit;
        private final K evictKey;

        private AdmissionDecision(boolean admit, K evictKey) {
            this.admit = admit;
            this.evictKey = evictKey;
        }

        public boolean shouldAdmit(){ return admit; }
        public K evictKey(){ return evictKey; }

        @SuppressWarnings("unchecked")
        static <K> AdmissionDecision<K> reject(){ return (AdmissionDecision<K>) REJECT; }
        @SuppressWarnings("unchecked")
        static <K> AdmissionDecision<K> admit(){ return (AdmissionDecision<K>) ADMIT_NO_EVICTION; }
        static <K> AdmissionDecision<K> admit(K evictKey){ return new AdmissionDecision<>(true, evictKey); }
    }
}

