package com.can.codec;

import java.nio.charset.StandardCharsets;

/**
 * Java {@link String} nesnelerini UTF-8 karakter setiyle kodlayıp çözen basit
 * codec implementasyonudur. Null değerleri boş diziye, boş dizileri ise boş
 * string'e çevirerek ağ ve disk operasyonlarında tutarlı davranış sergiler.
 */
public final class StringCodec implements Codec<String>
{
    public static final StringCodec UTF8 = new StringCodec();
    private StringCodec(){}

    @Override
    public byte[] encode(String obj) {
        return obj == null ? new byte[0] : obj.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String decode(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
