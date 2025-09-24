package com.can.core;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class StoredValueCodecTest
{
    @Nested
    class DecodeDavranisi
    {
        // Bu test encode edilmiş verinin çözümlenerek aynı alanları ürettiğini doğrular.
        @Test
        void decode_gecerli_veriyi_cozer()
        {
            StoredValueCodec.StoredValue stored = new StoredValueCodec.StoredValue(
                    "veri".getBytes(StandardCharsets.UTF_8), 7, 99L, 1_234L);
            String encoded = StoredValueCodec.encode(stored);
            StoredValueCodec.StoredValue decoded = StoredValueCodec.decode(encoded);
            assertArrayEquals(stored.value(), decoded.value());
            assertEquals(stored.flags(), decoded.flags());
            assertEquals(stored.cas(), decoded.cas());
            assertEquals(stored.expireAt(), decoded.expireAt());
            assertTrue(decoded.hasMetadata());
        }

        // Bu test bozuk Base64 girdisinin legacy formatı olarak ele alındığını gösterir.
        @Test
        void decode_hatali_veriyi_legacy_olarak_yorumlar()
        {
            StoredValueCodec.StoredValue decoded = StoredValueCodec.decode("not-base64");
            assertArrayEquals("not-base64".getBytes(StandardCharsets.UTF_8), decoded.value());
            assertFalse(decoded.hasMetadata());
            assertEquals(0L, decoded.cas());
            assertEquals(0L, decoded.expireAt());
        }

        // Bu test expireAt geçmiş olduğunda expired metodunun true döndüğünü doğrular.
        @Test
        void expired_metodu_suresi_gecmis_degerde_true_doner()
        {
            long past = System.currentTimeMillis() - 1_000L;
            StoredValueCodec.StoredValue stored = new StoredValueCodec.StoredValue(
                    "x".getBytes(StandardCharsets.UTF_8), 1, 5L, past);
            assertTrue(stored.expired(System.currentTimeMillis()));
        }
    }

    @Nested
    class MutasyonDavranisi
    {
        // Bu test withValue çağrısının yeni değer ve CAS ürettiğini gösterir.
        @Test
        void with_value_yeni_deger_ve_cas_atar()
        {
            StoredValueCodec.StoredValue stored = new StoredValueCodec.StoredValue(
                    "eski".getBytes(StandardCharsets.UTF_8), 2, 10L, 0L);
            StoredValueCodec.StoredValue mutated = stored.withValue("yeni".getBytes(StandardCharsets.UTF_8), 12L);
            assertArrayEquals("yeni".getBytes(StandardCharsets.UTF_8), mutated.value());
            assertEquals(12L, mutated.cas());
            assertEquals(stored.flags(), mutated.flags());
            assertEquals(stored.expireAt(), mutated.expireAt());
        }

        // Bu test withMeta çağrısının tüm alanları güncellediğini doğrular.
        @Test
        void with_meta_tum_alanlari_gunceller()
        {
            StoredValueCodec.StoredValue stored = new StoredValueCodec.StoredValue(
                    "veri".getBytes(StandardCharsets.UTF_8), 1, 3L, 4L);
            StoredValueCodec.StoredValue mutated = stored.withMeta(
                    "yeni".getBytes(StandardCharsets.UTF_8), 9, 7L, 8L);
            assertArrayEquals("yeni".getBytes(StandardCharsets.UTF_8), mutated.value());
            assertEquals(9, mutated.flags());
            assertEquals(7L, mutated.cas());
            assertEquals(8L, mutated.expireAt());
        }

        // Bu test withExpireAt çağrısının yalnızca süre bilgisini güncellediğini gösterir.
        @Test
        void with_expire_at_sadece_sureyi_degistirir()
        {
            StoredValueCodec.StoredValue stored = new StoredValueCodec.StoredValue(
                    "veri".getBytes(StandardCharsets.UTF_8), 1, 3L, 4L);
            StoredValueCodec.StoredValue mutated = stored.withExpireAt(55L, 66L);
            assertEquals(55L, mutated.expireAt());
            assertEquals(66L, mutated.cas());
            assertArrayEquals(stored.value(), mutated.value());
            assertEquals(stored.flags(), mutated.flags());
        }
    }
}
