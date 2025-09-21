package com.can.core;

import com.can.codec.StringCodec;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CacheEngineEvictionPolicyTest
{
    @Nested
    class DefaultPolicy {
        /**
         * Builder hiçbir politika verilmezse LRU kullanır; kapasite dolduğunda en eski giriş çıkar.
         * Bu test LRU davranışını doğrularken sıcak anahtarın sonradan silindiğini gözlemler.
         */
        @Test
        void builderDefaultsToLru()
        {
            try (CacheEngine<String,String> engine = CacheEngine.<String,String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1).maxCapacity(2)
                    .build())
            {
                engine.set("hot", "v");
                engine.set("cold-0", "v");
                engine.set("cold-1", "v");
                assertNull(engine.get("hot"));
            }
        }
    }

    @Nested
    class TinyLfuPolicy {
        /**
         * TinyLFU erişim sıklığına göre yeni adayları kabul eder; sıcak anahtar defalarca erişildiğinde korunmalıdır.
         * Test yoğun erişimden sonra yeni anahtarlar geldiğinde sıcak anahtarın hala erişilebilir olduğunu gösterir.
         */
        @Test
        void tinyLfuSelectionRetainsHotKeys()
        {
            try (CacheEngine<String,String> engine = CacheEngine.<String,String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1).maxCapacity(2)
                    .evictionPolicy(EvictionPolicyType.TINY_LFU)
                    .build())
            {
                engine.set("hot", "v");
                for (int i=0;i<128;i++) assertEquals("v", engine.get("hot"));
                engine.set("cold-0", "v");
                engine.set("cold-1", "v");
                assertEquals("v", engine.get("hot"));
                assertNull(engine.get("cold-1"));
            }
        }
    }
}

