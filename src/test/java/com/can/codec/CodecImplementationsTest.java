package com.can.codec;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.Serial;
import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Codec implementasyonları cache içerisinde veriyi byte dizisine çevirip geri döndürmek için kullanılır.
 * Her bir codec'in null güvenliği ve doğru dönüşümü sağladığını detaylı açıklamalarla test ediyoruz.
 */
class CodecImplementationsTest {

    @Nested
    class StringCodecBehaviour {
        /**
         * StringCodec UTF-8 ile kodlar, null değerleri boş diziye çevirir.
         * decode çağrısı boş diziyi yeniden null'a döndürerek orijinal anlamı korur.
         */
        @Test
        void encodesAndDecodesUtf8Strings() {
            byte[] encoded = StringCodec.UTF8.encode("merhaba");
            assertArrayEquals("merhaba".getBytes(java.nio.charset.StandardCharsets.UTF_8), encoded);
            assertEquals("merhaba", StringCodec.UTF8.decode(encoded));
        }

        @Test
        void handlesNullValuesGracefully() {
            assertArrayEquals(new byte[0], StringCodec.UTF8.encode(null));
            assertNull(StringCodec.UTF8.decode(new byte[0]));
        }
    }

    @Nested
    class JavaSerializerCodecBehaviour {
        /**
         * JavaSerializerCodec standart Java serileştirmesini kullanır.
         * encode-decode işlemi sırasında Serializable tiplerin durumu korunmalıdır.
         */
        @Test
        void roundTripsSerializableObjects() {
            JavaSerializerCodec<Dummy> codec = new JavaSerializerCodec<>();
            Dummy original = new Dummy("key", 42);
            byte[] bytes = codec.encode(original);
            Dummy decoded = codec.decode(bytes);
            assertEquals(original, decoded);
        }

        @Test
        void nullValuesProduceEmptyArray() {
            JavaSerializerCodec<String> codec = new JavaSerializerCodec<>();
            assertArrayEquals(new byte[0], codec.encode(null));
            assertNull(codec.decode(new byte[0]));
        }
    }

    private record Dummy(String name, int value) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }
}
