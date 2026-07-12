package com.cancache.agent.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * cancached text protokolü için değerleri CAS, bayrak ve TTL bilgileriyle birlikte
 * tek bir Base64 kodlu dize halinde serileştiren yardımcı sınıf. Ağ katmanı ile
 * önbellek motoru aynı kodu paylaşarak değerleri güvenli bir şekilde dekode edip
 * düzenleyebilir.
 */
public final class StoredValueCodec {
    private static final int MAGIC = 0x43435631; // CCV1
    private static final byte VERSION = 1;
    private static final int HEADER_SIZE = Integer.BYTES + Byte.BYTES + Integer.BYTES
            + Long.BYTES + Integer.BYTES + Long.BYTES;

    private StoredValueCodec() {
    }

    public static StoredValue decode(String encoded) {
        Objects.requireNonNull(encoded, "encoded");
        try {
            byte[] data = Base64.getDecoder().decode(encoded);
            if (data.length < HEADER_SIZE) {
                return legacy(encoded);
            }
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
            if (buffer.getInt() != MAGIC || buffer.get() != VERSION) {
                return legacy(encoded);
            }
            int valueLength = buffer.getInt();
            if (valueLength < 0 || valueLength != data.length - HEADER_SIZE) {
                return legacy(encoded);
            }
            long cas = buffer.getLong();
            int flags = buffer.getInt();
            long expireAt = buffer.getLong();
            byte[] value = new byte[valueLength];
            buffer.get(value);
            return new StoredValue(value, flags, cas, expireAt, true);
        } catch (IllegalArgumentException e) {
            return legacy(encoded);
        }
    }

    public static String encode(StoredValue value) {
        Objects.requireNonNull(value, "value");
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + value.value.length).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(MAGIC);
        buffer.put(VERSION);
        buffer.putInt(value.value.length);
        buffer.putLong(value.cas);
        buffer.putInt(value.flags);
        buffer.putLong(value.expireAt);
        buffer.put(value.value);
        return Base64.getEncoder().encodeToString(buffer.array());
    }

    private static StoredValue legacy(String raw) {
        byte[] bytes = raw.getBytes(StandardCharsets.UTF_8);
        return new StoredValue(bytes, 0, 0L, 0L, false);
    }

    public static final class StoredValue {
        private final byte[] value;
        private final int flags;
        private final long cas;
        private final long expireAt;
        private final boolean hasMetadata;

        public StoredValue(byte[] value, int flags, long cas, long expireAt) {
            this(value, flags, cas, expireAt, true);
        }

        private StoredValue(byte[] value, int flags, long cas, long expireAt, boolean hasMetadata) {
            this.value = Objects.requireNonNull(value, "value").clone();
            this.flags = flags;
            this.cas = cas;
            this.expireAt = expireAt;
            this.hasMetadata = hasMetadata;
        }

        public byte[] value() {
            return value.clone();
        }

        public int flags() {
            return flags;
        }

        public long cas() {
            return cas;
        }

        public long expireAt() {
            return expireAt;
        }

        public boolean hasMetadata() {
            return hasMetadata;
        }

        public boolean expired(long now) {
            return expireAt > 0 && expireAt != Long.MAX_VALUE && now >= expireAt;
        }

        public StoredValue withValue(byte[] newValue, long newCas) {
            return new StoredValue(newValue, flags, newCas, expireAt, true);
        }

        public StoredValue withMeta(byte[] newValue, int newFlags, long newCas, long newExpireAt) {
            return new StoredValue(newValue, newFlags, newCas, newExpireAt, true);
        }

        public StoredValue withExpireAt(long newExpireAt, long newCas) {
            return new StoredValue(value, flags, newCas, newExpireAt, true);
        }
    }
}
