package com.can.core;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CacheSegmentEvictionPolicyTest
{
    @Nested
    class TinyLfuVsLru {
        /**
         * TinyLFU sıcak anahtarların sıklık istatistiğini tutar ve soğuk anahtar baskısı altında kurban seçiminde avantaj sağlar.
         * Bu test binlerce erişim sonrasında aynı kapasitedeki LRU ile kıyaslayarak sıcak anahtarın TinyLFU'da hayatta kaldığını gösterir.
         */
        @Test
        void tinyLfuRejectsColdKeysUnderSkew()
        {
            CacheSegment<String> lru = new CacheSegment<>(2, new LruEvictionPolicy<>());
            CacheSegment<String> tiny = new CacheSegment<>(2, new TinyLfuEvictionPolicy<>(2));

            assertTrue(lru.put("hot", value()));
            assertTrue(tiny.put("hot", value()));
            for (int i=0;i<128;i++) assertNotNull(tiny.get("hot"));

            int lruAdmitted = 0, tinyAdmitted = 0;
            for (int i=0;i<64;i++)
            {
                String cold = "cold-" + i;
                if (lru.put(cold, value())) lruAdmitted++;
                if (tiny.put(cold, value())) tinyAdmitted++;
            }

            assertTrue(lruAdmitted > tinyAdmitted, "TinyLFU should admit fewer cold keys");
            assertNull(lru.get("hot"));
            assertNotNull(tiny.get("hot"));
        }
    }

    private static CacheValue value(){ return new CacheValue(new byte[]{1}, 0L); }
}

