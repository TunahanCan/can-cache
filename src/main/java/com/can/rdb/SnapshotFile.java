package com.can.rdb;

import com.can.codec.Codec;
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

public final class SnapshotFile<K, V> {

    private final File file;
    private final Codec<K> keyCodec;

    public SnapshotFile(File file, Codec<K> keyCodec) {
        this.file = file;
        this.keyCodec = keyCodec;
    }

    public synchronized void write(CacheEngine<K, V> engine) {
        try {
            Path temp = createTempFile();
            try (BufferedWriter writer = Files.newBufferedWriter(temp, StandardCharsets.UTF_8)) {
                try {
                    engine.forEachEntry((key, value, expireAt) -> {
                        try {
                            writer.write('S');
                            writer.write(' ');
                            writer.write(Base64.getEncoder().encodeToString(keyCodec.encode(key)));
                            writer.write(' ');
                            writer.write(Base64.getEncoder().encodeToString(value));
                            writer.write(' ');
                            writer.write(Long.toString(expireAt));
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
                String[] parts = line.split(" ");
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
