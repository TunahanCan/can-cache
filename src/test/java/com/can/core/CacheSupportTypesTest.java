package com.can.core;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Çekirdek paketindeki yardımcı tiplerin (CacheValue, ExpiringKey, EvictionPolicyType) davranışlarını belgeleyen testler.
 * Bu tipler TTL ve politika seçimi gibi kritik kontrol noktalarının bel kemiğini oluşturur.
 */
class CacheSupportTypesTest {

    @Nested
    class CacheValueExpiry {
        /**
         * expired(now) metodu expireAtMillis değeri 0 veya geçmişteyse true/false döndürür.
         * TTL'siz kayıtlar negatif veya sıfır değerde kalır ve hiçbir zaman expire olmaz.
         */
        @Test
        void reportsExpiryStateCorrectly() {
            CacheValue immortal = new CacheValue(new byte[]{1}, 0L);
            assertFalse(immortal.expired(System.currentTimeMillis()), "TTL tanımlanmayan kayıt geçersiz sayılmamalı");

            long past = System.currentTimeMillis() - 1_000;
            CacheValue stale = new CacheValue(new byte[]{1}, past);
            assertTrue(stale.expired(System.currentTimeMillis()), "Geçmiş zamanlı kayıt expire olmalı");

            long future = System.currentTimeMillis() + 1_000;
            CacheValue fresh = new CacheValue(new byte[]{1}, future);
            assertFalse(fresh.expired(System.currentTimeMillis()), "Gelecekte expire olacak kayıt henüz geçerli");
        }
    }

    @Nested
    class ExpiringKeyOrdering {
        /**
         * DelayQueue içerisinde kullanılmak üzere getDelay dinamik bir süre döndürür.
         * Süre azaldıkça değer de azalmalı, compareTo daha erken bitecek anahtarı öncelemelidir.
         */
        @Test
        void delayReflectsRemainingTimeAndOrdering() throws InterruptedException {
            long now = System.currentTimeMillis();
            ExpiringKey near = new ExpiringKey("a", 0, now + 50);
            ExpiringKey far = new ExpiringKey("b", 0, now + 200);

            long nearDelay = near.getDelay(TimeUnit.MILLISECONDS);
            assertTrue(nearDelay <= 60 && nearDelay > 0, "Yakın sürede bitecek anahtarın gecikmesi pozitif ve küçük olmalı");

            Thread.sleep(40);
            long reducedDelay = near.getDelay(TimeUnit.MILLISECONDS);
            assertTrue(reducedDelay < nearDelay, "Zaman geçtikçe gecikme küçülmeli");

            assertTrue(near.compareTo(far) < 0, "compareTo daha erken süresi dolacak anahtarı öncelemeli");
        }
    }

    @Nested
    class EvictionPolicyParsing {
        /**
         * fromConfig boş veya tanınmayan değerler geldiğinde varsayılan LRU'yu seçer.
         * Ayrıca farklı yazım biçimlerini normalize ederek TinyLFU gibi seçenekleri döndürmelidir.
         */
        @Test
        void parsesConfigValuesCaseInsensitive() {
            assertEquals(EvictionPolicyType.LRU, EvictionPolicyType.fromConfig(null));
            assertEquals(EvictionPolicyType.LRU, EvictionPolicyType.fromConfig("  "));
            assertEquals(EvictionPolicyType.TINY_LFU, EvictionPolicyType.fromConfig("tiny-lfu"));
        }

        @Test
        void throwsOnUnknownPolicy() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> EvictionPolicyType.fromConfig("unknown"));
            assertTrue(ex.getMessage().contains("Unknown eviction policy"));
        }
    }
}
