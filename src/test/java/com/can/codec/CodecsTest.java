package com.can.codec;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.*;

class CodecsTest
{
    @Nested
    class StringCodecDavranisi
    {
        // Bu test null değerin boş diziye dönüştürüldüğünü doğrular.
        @Test
        void encode_null_bos_dizi_doner()
        {
            assertArrayEquals(new byte[0], StringCodec.UTF8.encode(null));
        }

        // Bu test boş dizinin boş stringe çözüldüğünü gösterir.
        @Test
        void decode_bos_dizi_bos_string_doner()
        {
            assertEquals("", StringCodec.UTF8.decode(new byte[0]));
        }

        // Bu test encode-decode işleminin yuvarlak tur sağladığını doğrular.
        @Test
        void encode_decode_tam_tur_calismaya_devam_eder()
        {
            String original = "Merhaba dünya";
            byte[] encoded = StringCodec.UTF8.encode(original);
            assertEquals(original, StringCodec.UTF8.decode(encoded));
        }
    }

    @Nested
    class JavaSerializerCodecDavranisi
    {
        // Bu test serileştirilebilir nesnenin aynı içerikle geri döndüğünü doğrular.
        @Test
        void serialize_ve_deserialize_ayni_nesneyi_doner()
        {
            JavaSerializerCodec<Sample> codec = new JavaSerializerCodec<>();
            Sample original = new Sample("data", 42);
            byte[] bytes = codec.encode(original);
            Sample decoded = codec.decode(bytes);
            assertEquals(original, decoded);
        }

        // Bu test boş diziden null döndüğünü gösterir.
        @Test
        void decode_bos_dizi_null_doner()
        {
            JavaSerializerCodec<Sample> codec = new JavaSerializerCodec<>();
            assertNull(codec.decode(new byte[0]));
        }
    }

    private record Sample(String text, int number) implements Serializable {}
}
