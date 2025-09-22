package com.can.cache.integration;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal cancached text protocol client implemented in Java so that the
 * integration tests can exercise the can-cache service end to end without
 * relying on external libraries.
 */
public class MemcacheTextClient implements AutoCloseable {

    private static final String LOOPBACK_HOST = "127.0.0.1";

    public static final String DEFAULT_HOST = resolveDefaultHost();
    public static final int DEFAULT_PORT = resolveDefaultPort();
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

    private static volatile String currentHost = DEFAULT_HOST;
    private static volatile int currentPort = DEFAULT_PORT;

    private final String host;
    private final int port;
    private final Duration timeout;

    private Socket socket;
    private BufferedInputStream input;
    private BufferedOutputStream output;

    public MemcacheTextClient() {
        this(currentHost, currentPort, DEFAULT_TIMEOUT);
    }

    public MemcacheTextClient(String host, int port, Duration timeout) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
    }

    public void connect() throws IOException {
        if (socket != null) {
            return;
        }
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), (int) timeout.toMillis());
        socket.setSoTimeout((int) timeout.toMillis());
        input = new BufferedInputStream(socket.getInputStream());
        output = new BufferedOutputStream(socket.getOutputStream());
    }

    private void ensureConnected() {
        if (socket == null || !socket.isConnected()) {
            throw new IllegalStateException("Client is not connected");
        }
    }

    @Override
    public void close() {
        try {
            if (input != null) {
                input.close();
            }
        } catch (IOException ignored) {
            // Ignore close errors in tear down paths.
        }
        try {
            if (output != null) {
                output.close();
            }
        } catch (IOException ignored) {
            // Ignore close errors in tear down paths.
        }
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
            // Ignore close errors in tear down paths.
        }
        input = null;
        output = null;
        socket = null;
    }

    private void sendLine(String line) throws IOException {
        ensureConnected();
        byte[] data = (line + "\r\n").getBytes(StandardCharsets.UTF_8);
        output.write(data);
        output.flush();
    }

    private void sendDataBlock(byte[] payload) throws IOException {
        ensureConnected();
        output.write(payload);
        output.write('\r');
        output.write('\n');
        output.flush();
    }

    private String readLine() throws IOException {
        ensureConnected();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        while (true) {
            int b = input.read();
            if (b == -1) {
                throw new EOFException("Connection closed by server");
            }
            if (b == '\r') {
                int next = input.read();
                if (next != '\n') {
                    throw new IOException("Protocol violation: expected LF after CR");
                }
                break;
            }
            if (b == '\n') {
                break;
            }
            buffer.write(b);
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    private byte[] readExact(int length) throws IOException {
        ensureConnected();
        byte[] data = input.readNBytes(length);
        if (data.length != length) {
            throw new EOFException("Unexpected end of stream");
        }
        int cr = input.read();
        int lf = input.read();
        if (cr != '\r' || lf != '\n') {
            throw new IOException("Protocol violation: missing CRLF after data block");
        }
        return data;
    }

    public String storageCommand(String verb, String key, String value, int flags, int exptime) throws IOException {
        return storageCommand(verb, key, value, flags, exptime, false);
    }

    public String storageCommand(String verb, String key, String value, int flags, int exptime, boolean noreply) throws IOException {
        byte[] payload = value.getBytes(StandardCharsets.UTF_8);
        StringBuilder builder = new StringBuilder();
        builder.append(verb)
            .append(' ').append(key)
            .append(' ').append(flags)
            .append(' ').append(exptime)
            .append(' ').append(payload.length);
        if (noreply) {
            builder.append(" noreply");
        }
        sendLine(builder.toString());
        sendDataBlock(payload);
        if (noreply) {
            return "";
        }
        return readLine();
    }

    public String set(String key, String value, int flags, int exptime) throws IOException {
        return storageCommand("set", key, value, flags, exptime);
    }

    public String add(String key, String value, int flags, int exptime) throws IOException {
        return storageCommand("add", key, value, flags, exptime);
    }

    public String replace(String key, String value, int flags, int exptime) throws IOException {
        return storageCommand("replace", key, value, flags, exptime);
    }

    public String append(String key, String value) throws IOException {
        return storageCommand("append", key, value, 0, 0);
    }

    public String prepend(String key, String value) throws IOException {
        return storageCommand("prepend", key, value, 0, 0);
    }

    public String cas(String key, String value, long casToken, int flags, int exptime) throws IOException {
        byte[] payload = value.getBytes(StandardCharsets.UTF_8);
        String command = String.format("cas %s %d %d %d %d", key, flags, exptime, payload.length, casToken);
        sendLine(command);
        sendDataBlock(payload);
        return readLine();
    }

    public String delete(String key) throws IOException {
        sendLine("delete " + key);
        return readLine();
    }

    public Long incr(String key, long delta) throws IOException {
        sendLine("incr " + key + " " + delta);
        return parseNumericResponse(readLine());
    }

    public Long decr(String key, long delta) throws IOException {
        sendLine("decr " + key + " " + delta);
        return parseNumericResponse(readLine());
    }

    private Long parseNumericResponse(String response) {
        for (int i = 0; i < response.length(); i++) {
            if (!Character.isDigit(response.charAt(i))) {
                return null;
            }
        }
        if (response.isEmpty()) {
            return null;
        }
        return Long.parseLong(response);
    }

    public String touch(String key, int exptime) throws IOException {
        sendLine("touch " + key + " " + exptime);
        return readLine();
    }

    public String flushAll() throws IOException {
        sendLine("flush_all");
        return readLine();
    }

    public Map<String, String> stats() throws IOException {
        sendLine("stats");
        Map<String, String> stats = new LinkedHashMap<>();
        while (true) {
            String line = readLine();
            if ("END".equals(line)) {
                return stats;
            }
            String[] parts = line.split(" ", 3);
            if (parts.length == 3 && "STAT".equals(parts[0])) {
                stats.put(parts[1], parts[2]);
            }
        }
    }

    public String version() throws IOException {
        sendLine("version");
        return readLine();
    }

    public Map<String, ValueRecord> get(String... keys) throws IOException {
        return getMany("get", keys);
    }

    public Map<String, ValueRecord> gets(String... keys) throws IOException {
        return getMany("gets", keys);
    }

    private Map<String, ValueRecord> getMany(String command, String... keys) throws IOException {
        if (keys.length == 0) {
            return Map.of();
        }
        String joinedKeys = String.join(" ", keys);
        sendLine(command + " " + joinedKeys);
        Map<String, ValueRecord> results = new LinkedHashMap<>();
        while (true) {
            String line = readLine();
            if ("END".equals(line)) {
                return results;
            }
            String[] parts = line.split(" ");
            if (parts.length < 4 || !"VALUE".equals(parts[0])) {
                throw new IOException("Unexpected response: " + line);
            }
            String key = parts[1];
            int flags = Integer.parseInt(parts[2]);
            int length = Integer.parseInt(parts[3]);
            Long cas = parts.length >= 5 ? Long.parseLong(parts[4]) : null;
            byte[] data = readExact(length);
            String value = new String(data, StandardCharsets.UTF_8);
            results.put(key, new ValueRecord(key, flags, value, cas));
        }
    }

    public static void waitForService(Duration timeout) throws InterruptedException {
        waitForService(currentHost, currentPort, timeout);
    }

    public static void waitForService(String host, int port, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        Duration connectTimeout = Duration.ofSeconds(1);
        while (System.nanoTime() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), (int) connectTimeout.toMillis());
                return;
            } catch (IOException ex) {
                Thread.sleep(500);
            }
        }
        throw new IllegalStateException("Service " + host + ":" + port + " did not become ready within " + timeout);
    }

    public record ValueRecord(String key, int flags, String value, Long casToken) {
    }

    static String configuredHost() {
        return DEFAULT_HOST;
    }

    static int configuredPort() {
        return DEFAULT_PORT;
    }

    static synchronized void overrideDefaultEndpoint(String host, int port) {
        currentHost = sanitizeHost(host);
        currentPort = validatePort(port);
    }

    static String loopbackHost() {
        return LOOPBACK_HOST;
    }

    private static String resolveDefaultHost() {
        String host = System.getenv("CAN_CACHE_HOST");
        return sanitizeHost(host);
    }

    private static int resolveDefaultPort() {
        String value = System.getenv("CAN_CACHE_PORT");
        if (value == null || value.isBlank()) {
            return 11211;
        }
        try {
            return validatePort(Integer.parseInt(value.trim(), 10));
        } catch (NumberFormatException | IllegalArgumentException ex) {
            return 11211;
        }
    }

    private static String sanitizeHost(String host) {
        if (host == null) {
            return LOOPBACK_HOST;
        }
        String trimmed = host.trim();
        return trimmed.isEmpty() ? LOOPBACK_HOST : trimmed;
    }

    private static int validatePort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port out of range: " + port);
        }
        return port;
    }
}
