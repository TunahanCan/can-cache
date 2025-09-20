package com.can.aof;

import com.can.codec.Codec;
import com.can.core.CacheEngine;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

/** Satır tabanlı basit AOF: "S|D <b64key> <b64val> <expireAtMillis>\n" */


public final class AppendOnlyFile<K,V> implements Closeable {
    private final File file;
    private final Codec<K> keyCodec;
    private final Codec<V> valCodec;
    private FileChannel channel;
    private final boolean fsyncEvery;

    public AppendOnlyFile(File file, Codec<K> keyCodec, Codec<V> valCodec, boolean fsyncEvery) {
        this.file = file; this.keyCodec = keyCodec; this.valCodec = valCodec; this.fsyncEvery = fsyncEvery;
        try {
            this.channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    public synchronized void appendSet(K key, V value, long expireAtMillis) { writeLine('S', keyCodec.encode(key), valCodec.encode(value), expireAtMillis); }
    public synchronized void appendDel(K key) { writeLine('D', keyCodec.encode(key), new byte[0], 0); }

    private void writeLine(char op, byte[] key, byte[] val, long expireAt) {
        String line = op + " " + Base64.getEncoder().encodeToString(key) + " " + Base64.getEncoder().encodeToString(val) + " " + expireAt + "\n";
        try { channel.write(ByteBuffer.wrap(line.getBytes())); if (fsyncEvery) channel.force(true); }
        catch (IOException e) { throw new RuntimeException(e); }
    }

    public static <K,V> void replay(File file, CacheEngine<K,V> engine) {
        if (!file.exists()) return;
        try (var br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] p = line.split(" ");
                char op = p[0].charAt(0);
                byte[] k = Base64.getDecoder().decode(p[1]);
                byte[] v = Base64.getDecoder().decode(p[2]);
                long ex = Long.parseLong(p[3]);
                engine.replay(new byte[]{(byte)op}, k, v, ex);
            }
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    @Override public void close(){ try { if (channel != null) channel.close(); } catch (IOException ignored) {} }
}
