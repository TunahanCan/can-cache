package com.can.core;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class StoredValueCodecTest
{
    @Nested
    class DecodeBehaviour
    {
        @Test
        void decode_returns_legacy_when_not_base64()
        {
            StoredValueCodec.StoredValue value = StoredValueCodec.decode("not-base64");
            assertFalse(value.hasMetadata());
            assertArrayEquals("not-base64".getBytes(StandardCharsets.UTF_8), value.value());
        }

        @Test
        void decode_returns_legacy_when_payload_too_short()
        {
            String encoded = java.util.Base64.getEncoder().encodeToString(new byte[]{1, 2, 3});
            StoredValueCodec.StoredValue value = StoredValueCodec.decode(encoded);
            assertFalse(value.hasMetadata());
            assertArrayEquals(new byte[]{1, 2, 3}, value.value());
        }

        @Test
        void decode_roundtrip_preserves_fields()
        {
            byte[] payload = "value".getBytes(StandardCharsets.UTF_8);
            StoredValueCodec.StoredValue original = new StoredValueCodec.StoredValue(payload, 7, 42L, 1234L);
            String encoded = StoredValueCodec.encode(original);
            StoredValueCodec.StoredValue decoded = StoredValueCodec.decode(encoded);

            assertTrue(decoded.hasMetadata());
            assertArrayEquals(payload, decoded.value());
            assertEquals(7, decoded.flags());
            assertEquals(42L, decoded.cas());
            assertEquals(1234L, decoded.expireAt());
        }
    }

    @Nested
    class MutationOperations
    {
        @Test
        void with_value_updates_payload_and_cas()
        {
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue("v1".getBytes(StandardCharsets.UTF_8), 1, 10L, 100L);
            StoredValueCodec.StoredValue updated = base.withValue("v2".getBytes(StandardCharsets.UTF_8), 11L);

            assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8), updated.value());
            assertEquals(11L, updated.cas());
            assertEquals(1, updated.flags());
            assertEquals(100L, updated.expireAt());
        }

        @Test
        void with_meta_replaces_all_fields()
        {
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue("v1".getBytes(StandardCharsets.UTF_8), 1, 10L, 100L);
            StoredValueCodec.StoredValue updated = base.withMeta("v3".getBytes(StandardCharsets.UTF_8), 5, 99L, 200L);

            assertArrayEquals("v3".getBytes(StandardCharsets.UTF_8), updated.value());
            assertEquals(5, updated.flags());
            assertEquals(99L, updated.cas());
            assertEquals(200L, updated.expireAt());
        }

        @Test
        void with_expire_at_updates_only_expiration()
        {
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue("v1".getBytes(StandardCharsets.UTF_8), 1, 10L, 100L);
            StoredValueCodec.StoredValue updated = base.withExpireAt(500L, 12L);

            assertArrayEquals(base.value(), updated.value());
            assertEquals(1, updated.flags());
            assertEquals(12L, updated.cas());
            assertEquals(500L, updated.expireAt());
        }

        @Test
        void expired_returns_true_when_now_beyond_expire()
        {
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue(new byte[]{1}, 0, 0L, System.currentTimeMillis() - 1);
            assertTrue(base.expired(System.currentTimeMillis()));
        }

        @Test
        void expired_ignores_max_value()
        {
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue(new byte[]{1}, 0, 0L, Long.MAX_VALUE);
            assertFalse(base.expired(System.currentTimeMillis()));
        }
    }
}
