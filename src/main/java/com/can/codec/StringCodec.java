package com.can.codec;

import java.nio.charset.StandardCharsets;

/**
 * Java {@link String} nesnelerini UTF-8 karakter setiyle kodlayıp çözen basit
 * codec implementasyonudur. Null değerleri boş diziye/boş referansa çevirerek
 * ağ ve disk operasyonlarında tutarlı davranış sergiler.
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
        return (bytes == null || bytes.length == 0) ? null : new String(bytes, StandardCharsets.UTF_8);
    }
}