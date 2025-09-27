package com.can.rdb;

import com.can.codec.Codec;
import com.can.constants.NodeProtocol;
import com.can.core.CacheEngine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;

/**
 * Önbellek içeriğini kalıcı hale getirmek amacıyla RDB benzeri düz dosya formatı
 * üzerinden okuma/yazma işlemlerini üstlenen kayıt sınıfıdır. Anahtarları verilen
 * codec ile kodlayıp Base64'e çevirerek satır satır diske yazar, uygulama
 * yeniden başladığında ise aynı dosyayı okuyup {@link CacheEngine} üzerinde
 * komutları tekrar oynatarak belleği geri yükler.
 */
public record SnapshotFile<K, V>(File file, Codec<K> keyCodec) {

    /**
     * Tab karakteri Base64 çıktılarında bulunmadığı için alan ayırıcı olarak güvenle kullanılabilir.
     */
    private static final char FIELD_SEPARATOR = '\t';
    private static final String FIELD_SEPARATOR_STRING = String.valueOf(FIELD_SEPARATOR);
    private static final String FIELD_SEPARATOR_REGEX = java.util.regex.Pattern.quote(FIELD_SEPARATOR_STRING);

    public synchronized void write(CacheEngine<K, V> engine) {
        try {
            Path temp = createTempFile();
            try (BufferedWriter writer = Files.newBufferedWriter(temp, StandardCharsets.UTF_8)) {
                try {
                    engine.forEachEntry((key, value, expireAt) -> {
                        try {
                            String encodedKey = Base64.getEncoder().encodeToString(keyCodec.encode(key));
                            String encodedValue = Base64.getEncoder().encodeToString(value);
                            String line = String.join(
                                FIELD_SEPARATOR_STRING,
                                Character.toString((char) NodeProtocol.CMD_SET),
                                encodedKey,
                                encodedValue,
                                Long.toString(expireAt)
                            );
                            writer.write(line);
                            writer.newLine();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
                } catch (UncheckedIOException e) {
                    throw new IOException(e.getCause());
                }
            }

            try {
                Files.move(temp, file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void load(CacheEngine<K, V> engine) {
        if (!file.exists()) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                String[] parts = line.split(FIELD_SEPARATOR_REGEX, 4);
                if (parts.length < 4) {
                    continue;
                }
                try {
                    char op = parts[0].charAt(0);
                    byte[] key = Base64.getDecoder().decode(parts[1]);
                    byte[] value = Base64.getDecoder().decode(parts[2]);
                    long expireAt = Long.parseLong(parts[3]);
                    engine.replay(new byte[]{(byte) op}, key, value, expireAt);
                } catch (IllegalArgumentException | ArrayIndexOutOfBoundsException ignored) {
                    // skip malformed lines
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path createTempFile() throws IOException {
        File parent = file.getAbsoluteFile().getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
        if (parent != null) {
            return Files.createTempFile(parent.toPath(), file.getName(), ".tmp");
        }
        return Files.createTempFile(file.getName(), ".tmp");
    }
}
