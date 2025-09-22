package com.can.codec;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.Serial;
import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.*;

class CodecsTest
{
    @Nested
    class StringCodecBehaviour
    {
        @Test
        void encode_and_decode_roundtrip()
        {
            StringCodec codec = StringCodec.UTF8;
            byte[] encoded = codec.encode("hello");
            assertEquals("hello", codec.decode(encoded));
        }

        @Test
        void encode_and_decode_handles_null()
        {
            StringCodec codec = StringCodec.UTF8;
            assertArrayEquals(new byte[0], codec.encode(null));
            assertNull(codec.decode(new byte[0]));
        }
    }

    @Nested
    class JavaSerializerCodecBehaviour
    {
        @Test
        void encode_and_decode_serializable_object()
        {
            JavaSerializerCodec<Dummy> codec = new JavaSerializerCodec<>();
            Dummy original = new Dummy("value", 42);
            byte[] encoded = codec.encode(original);
            Dummy decoded = codec.decode(encoded);

            assertEquals(original.name, decoded.name);
            assertEquals(original.count, decoded.count);
        }

        @Test
        void decode_returns_null_for_empty_array()
        {
            JavaSerializerCodec<Dummy> codec = new JavaSerializerCodec<>();
            assertNull(codec.decode(new byte[0]));
        }
    }

    private record Dummy(String name, int count) implements Serializable
    {
        @Serial
        private static final long serialVersionUID = 1L;
    }
}
