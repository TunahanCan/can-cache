package com.cancache.agent.core;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class StoredValueCodecTest
{
    @Nested
    class DecodeBehavior
    {
        /**
         * Verifies that decoded data produces the same fields as the encoded valid value.
         */
        @Test
        void shouldDecodeValidValueSuccessfully()
        {
            // Given
            StoredValueCodec.StoredValue stored = new StoredValueCodec.StoredValue(
                    "veri".getBytes(StandardCharsets.UTF_8), 7, 99L, 1_234L);
            String encoded = StoredValueCodec.encode(stored);
            
            // When
            StoredValueCodec.StoredValue decoded = StoredValueCodec.decode(encoded);
            
            // Then
            assertArrayEquals(stored.value(), decoded.value(), "Values should match");
            assertEquals(stored.flags(), decoded.flags(), "Flags should match");
            assertEquals(stored.cas(), decoded.cas(), "CAS tokens should match");
            assertEquals(stored.expireAt(), decoded.expireAt(), "Expiration times should match");
            assertTrue(decoded.hasMetadata(), "Valid decoded value should have metadata");
        }

        /**
         * Shows that invalid Base64 input is interpreted as the legacy format.
         */
        @Test
        void shouldInterpretInvalidValueAsLegacy()
        {
            // Given
            String input = "not-base64";
            
            // When
            StoredValueCodec.StoredValue decoded = StoredValueCodec.decode(input);
            
            // Then
            assertArrayEquals("not-base64".getBytes(StandardCharsets.UTF_8), decoded.value(), "Value should be the verbatim input bytes");
            assertFalse(decoded.hasMetadata(), "Legacy values should not have metadata");
            assertEquals(0L, decoded.cas(), "Legacy CAS should be 0");
            assertEquals(0L, decoded.expireAt(), "Legacy expiration should be 0");
        }

        @Test
        void shouldNotMistakeValidBase64LegacyTextForMetadata()
        {
            String input = Base64.getEncoder().encodeToString(new byte[32]);

            StoredValueCodec.StoredValue decoded = StoredValueCodec.decode(input);

            assertFalse(decoded.hasMetadata(), "Only framed values with magic and version are metadata");
            assertArrayEquals(input.getBytes(StandardCharsets.UTF_8), decoded.value());
        }

        /**
         * Verifies that the expired method returns true when the expiration time is in the past.
         */
        @Test
        void shouldReturnTrueForPastValueOnExpired()
        {
            // Given
            long past = System.currentTimeMillis() - 1_000L;
            StoredValueCodec.StoredValue stored = new StoredValueCodec.StoredValue(
                    "x".getBytes(StandardCharsets.UTF_8), 1, 5L, past);
            
            // When
            boolean isExpired = stored.expired(System.currentTimeMillis());
            
            // Then
            assertTrue(isExpired, "Stored value with past expiration should be considered expired");
        }
    }

    @Nested
    class MutationBehavior
    {
        /**
         * Shows that calling withValue creates a new instance with updated value and CAS.
         */
        @Test
        void shouldUpdateValueAndCasOnWithValue()
        {
            // Given
            StoredValueCodec.StoredValue stored = new StoredValueCodec.StoredValue(
                    "eski".getBytes(StandardCharsets.UTF_8), 2, 10L, 0L);
            
            // When
            StoredValueCodec.StoredValue mutated = stored.withValue("yeni".getBytes(StandardCharsets.UTF_8), 12L);
            
            // Then
            assertArrayEquals("yeni".getBytes(StandardCharsets.UTF_8), mutated.value(), "Value should be updated");
            assertEquals(12L, mutated.cas(), "CAS should be updated");
            assertEquals(stored.flags(), mutated.flags(), "Flags should remain unchanged");
            assertEquals(stored.expireAt(), mutated.expireAt(), "Expiration should remain unchanged");
        }

        /**
         * Verifies that calling withMeta updates all fields.
         */
        @Test
        void shouldUpdateAllFieldsOnWithMeta()
        {
            // Given
            StoredValueCodec.StoredValue stored = new StoredValueCodec.StoredValue(
                    "veri".getBytes(StandardCharsets.UTF_8), 1, 3L, 4L);
            
            // When
            StoredValueCodec.StoredValue mutated = stored.withMeta(
                    "yeni".getBytes(StandardCharsets.UTF_8), 9, 7L, 8L);
            
            // Then
            assertArrayEquals("yeni".getBytes(StandardCharsets.UTF_8), mutated.value(), "Value should be updated");
            assertEquals(9, mutated.flags(), "Flags should be updated");
            assertEquals(7L, mutated.cas(), "CAS should be updated");
            assertEquals(8L, mutated.expireAt(), "Expiration should be updated");
        }

        /**
         * Shows that calling withExpireAt only updates the expiration and CAS.
         */
        @Test
        void shouldOnlyChangeExpirationAndCasOnWithExpireAt()
        {
            // Given
            StoredValueCodec.StoredValue stored = new StoredValueCodec.StoredValue(
                    "veri".getBytes(StandardCharsets.UTF_8), 1, 3L, 4L);
            
            // When
            StoredValueCodec.StoredValue mutated = stored.withExpireAt(55L, 66L);
            
            // Then
            assertEquals(55L, mutated.expireAt(), "Expiration should be updated");
            assertEquals(66L, mutated.cas(), "CAS should be updated");
            assertArrayEquals(stored.value(), mutated.value(), "Value should remain unchanged");
            assertEquals(stored.flags(), mutated.flags(), "Flags should remain unchanged");
        }

        @Test
        void shouldDefensivelyCopyPayloadArrays()
        {
            byte[] source = "value".getBytes(StandardCharsets.UTF_8);
            StoredValueCodec.StoredValue stored = new StoredValueCodec.StoredValue(source, 0, 1L, 0L);
            source[0] = 'X';
            byte[] exposed = stored.value();
            exposed[1] = 'X';

            assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), stored.value());
        }
    }
}
