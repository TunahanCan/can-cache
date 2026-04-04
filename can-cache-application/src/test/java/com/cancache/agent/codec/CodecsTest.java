package com.cancache.agent.codec;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.*;

class CodecsTest
{
    @Nested
    class StringCodecBehavior
    {
        /**
         * Verifies that encoding a null value yields an empty array.
         */
        @Test
        void shouldReturnEmptyArrayWhenEncodingNull()
        {
            // Given / When
            byte[] result = StringCodec.UTF8.encode(null);
            
            // Then
            assertArrayEquals(new byte[0], result, "Encoding null should result in an empty byte array");
        }

        /**
         * Shows that an empty array decodes into an empty string.
         */
        @Test
        void shouldReturnEmptyStringWhenDecodingEmptyArray()
        {
            // Given / When
            String result = StringCodec.UTF8.decode(new byte[0]);
            
            // Then
            assertEquals("", result, "Decoding an empty byte array should return an empty string");
        }

        /**
         * Verifies that an encode-decode round trip preserves the original string.
         */
        @Test
        void shouldPerformRoundTripEncodeDecode()
        {
            // Given
            String original = "Merhaba dünya";
            
            // When
            byte[] encoded = StringCodec.UTF8.encode(original);
            String decoded = StringCodec.UTF8.decode(encoded);
            
            // Then
            assertEquals(original, decoded, "Decoded string should match the original after round-trip");
        }
    }

    @Nested
    class JavaSerializerCodecBehavior
    {
        /**
         * Verifies that a serializable object is returned with identical content after serialization and deserialization.
         */
        @Test
        void shouldReturnIdenticalObjectAfterSerializeAndDeserialize()
        {
            // Given
            JavaSerializerCodec<Sample> codec = new JavaSerializerCodec<>();
            Sample original = new Sample("data", 42);
            
            // When
            byte[] bytes = codec.encode(original);
            Sample decoded = codec.decode(bytes);
            
            // Then
            assertEquals(original, decoded, "Decoded object should be identical to the original");
        }

        /**
         * Shows that decoding an empty array returns null.
         */
        @Test
        void shouldReturnNullWhenDecodingEmptyArray()
        {
            // Given
            JavaSerializerCodec<Sample> codec = new JavaSerializerCodec<>();
            
            // When
            Sample result = codec.decode(new byte[0]);
            
            // Then
            assertNull(result, "Decoding an empty byte array should result in null");
        }
    }

    private record Sample(String text, int number) implements Serializable {}
}
