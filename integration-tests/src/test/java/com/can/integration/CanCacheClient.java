package com.can.integration;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Minimal memcached-compatible client tailored for integration tests.
 */
final class CanCacheClient implements Closeable
{
    private static final byte[] CRLF = new byte[]{'\r', '\n'};
    private final Socket socket;
    private final InputStream input;
    private final OutputStream output;

    private CanCacheClient(Socket socket) throws IOException
    {
        this.socket = socket;
        this.input = socket.getInputStream();
        this.output = socket.getOutputStream();
    }

    static CanCacheClient connectDefault() throws IOException
    {
        String host = Optional.ofNullable(System.getenv("CAN_CACHE_HOST")).orElse("127.0.0.1");
        int port = Integer.parseInt(Optional.ofNullable(System.getenv("CAN_CACHE_PORT")).orElse("11211"));
        Socket socket = new Socket();
        socket.setSoTimeout(5000);
        socket.connect(new InetSocketAddress(host, port), 5000);
        return new CanCacheClient(socket);
    }

    String set(String key, int flags, long exptimeSeconds, String value) throws IOException
    {
        return store("set", key, flags, exptimeSeconds, value.getBytes(StandardCharsets.UTF_8), null);
    }

    String add(String key, int flags, long exptimeSeconds, String value) throws IOException
    {
        return store("add", key, flags, exptimeSeconds, value.getBytes(StandardCharsets.UTF_8), null);
    }

    String replace(String key, int flags, long exptimeSeconds, String value) throws IOException
    {
        return store("replace", key, flags, exptimeSeconds, value.getBytes(StandardCharsets.UTF_8), null);
    }

    String append(String key, String value) throws IOException
    {
        return store("append", key, 0, 0L, value.getBytes(StandardCharsets.UTF_8), null);
    }

    String prepend(String key, String value) throws IOException
    {
        return store("prepend", key, 0, 0L, value.getBytes(StandardCharsets.UTF_8), null);
    }

    String cas(String key, int flags, long exptimeSeconds, String value, long cas) throws IOException
    {
        return store("cas", key, flags, exptimeSeconds, value.getBytes(StandardCharsets.UTF_8), cas);
    }

    String delete(String key) throws IOException
    {
        sendLine("delete " + key);
        return readLine();
    }

    String incr(String key, long delta) throws IOException
    {
        sendLine("incr " + key + " " + delta);
        return readLine();
    }

    String decr(String key, long delta) throws IOException
    {
        sendLine("decr " + key + " " + delta);
        return readLine();
    }

    String touch(String key, long exptimeSeconds) throws IOException
    {
        sendLine("touch " + key + " " + exptimeSeconds);
        return readLine();
    }

    String flushAll() throws IOException
    {
        sendLine("flush_all");
        return readLine();
    }

    String flushAll(Duration delay) throws IOException
    {
        Objects.requireNonNull(delay, "delay");
        long seconds = Math.max(0L, delay.toSeconds());
        sendLine("flush_all " + seconds);
        return readLine();
    }

    Map<String, CacheValue> getValues(String... keys) throws IOException
    {
        return multiGet("get", keys);
    }

    Optional<CacheValue> getValue(String key) throws IOException
    {
        return Optional.ofNullable(multiGet("get", key).get(key));
    }

    Map<String, CacheValue> gets(String... keys) throws IOException
    {
        return multiGet("gets", keys);
    }

    Map<String, String> stats() throws IOException
    {
        sendLine("stats");
        Map<String, String> result = new LinkedHashMap<>();
        while (true) {
            String line = readLine();
            if ("END".equals(line)) {
                return result;
            }
            if (!line.startsWith("STAT ")) {
                throw new IOException("Unexpected stats response: " + line);
            }
            String[] parts = line.split(" ", 3);
            if (parts.length != 3) {
                throw new IOException("Malformed stats line: " + line);
            }
            result.put(parts[1], parts[2]);
        }
    }

    String version() throws IOException
    {
        sendLine("version");
        return readLine();
    }

    private Map<String, CacheValue> multiGet(String command, String... keys) throws IOException
    {
        if (keys == null || keys.length == 0) {
            throw new IllegalArgumentException("At least one key must be provided");
        }
        String joinedKeys = String.join(" ", keys);
        sendLine(command + " " + joinedKeys);
        Map<String, CacheValue> result = new LinkedHashMap<>();
        while (true) {
            String line = readLine();
            if ("END".equals(line)) {
                return result;
            }
            if (!line.startsWith("VALUE ")) {
                throw new IOException("Unexpected get response: " + line);
            }
            String[] parts = line.split(" ");
            if (parts.length < 4) {
                throw new IOException("Malformed value header: " + line);
            }
            String key = parts[1];
            int flags = Integer.parseInt(parts[2]);
            int bytes = Integer.parseInt(parts[3]);
            Long cas = null;
            if (parts.length >= 5) {
                cas = Long.parseUnsignedLong(parts[4]);
            }
            byte[] data = readBytes(bytes);
            result.put(key, new CacheValue(key, flags, data, cas));
        }
    }

    private String store(String command,
                         String key,
                         int flags,
                         long exptimeSeconds,
                         byte[] value,
                         Long cas) throws IOException
    {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (exptimeSeconds < 0) {
            throw new IllegalArgumentException("exptimeSeconds cannot be negative");
        }
        StringBuilder header = new StringBuilder();
        header.append(command)
                .append(' ').append(key)
                .append(' ').append(flags)
                .append(' ').append(exptimeSeconds)
                .append(' ').append(value.length);
        if (cas != null) {
            header.append(' ').append(Long.toUnsignedString(cas));
        }
        sendLine(header.toString());
        output.write(value);
        output.write(CRLF);
        output.flush();
        return readLine();
    }

    private void sendLine(String command) throws IOException
    {
        output.write(command.getBytes(StandardCharsets.US_ASCII));
        output.write(CRLF);
        output.flush();
    }

    private String readLine() throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        while (true) {
            int b;
            try {
                b = input.read();
            } catch (SocketTimeoutException e) {
                throw new IOException("Timed out waiting for server response", e);
            }
            if (b == -1) {
                throw new EOFException("Connection closed by server");
            }
            if (b == '\r') {
                int next = input.read();
                if (next != '\n') {
                    throw new IOException("Expected LF after CR but got: " + next);
                }
                return buffer.toString(StandardCharsets.US_ASCII);
            }
            buffer.write(b);
        }
    }

    private byte[] readBytes(int length) throws IOException
    {
        byte[] data = input.readNBytes(length);
        if (data.length != length) {
            throw new EOFException("Expected " + length + " bytes but received " + data.length);
        }
        int cr = input.read();
        int lf = input.read();
        if (cr != '\r' || lf != '\n') {
            throw new IOException("Expected CRLF after value payload");
        }
        return data;
    }

    @Override
    public void close() throws IOException
    {
        try {
            sendLine("quit");
        } catch (IOException ignored) {
            // Best effort to gracefully close the connection.
        }
        socket.close();
    }

    record CacheValue(String key, int flags, byte[] data, Long cas)
    {
        String asString()
        {
            return new String(data, StandardCharsets.UTF_8);
        }

        long asLong()
        {
            return Long.parseLong(asString());
        }

        @Override
        public String toString()
        {
            return "CacheValue{" +
                    "key='" + key + '\'' +
                    ", flags=" + flags +
                    ", data=" + Arrays.toString(data) +
                    ", cas=" + cas +
                    '}';
        }
    }
}
