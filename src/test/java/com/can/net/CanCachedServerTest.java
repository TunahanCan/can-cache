package com.can.net;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class CanCachedServerTest {

    @Inject
    CanCachedServer server;

    @Test
    void setGetDeleteCycle() throws Exception {
        assertNotNull(server);
        try (Socket socket = connect();
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            flushAll(out, in);

            sendStorage(out, "set", "alpha", "value", 42, 0);
            assertEquals("STORED", readLine(in));

            sendGet(out, "alpha");
            assertEquals("VALUE alpha 42 5", readLine(in));
            assertEquals("value", readLine(in));
            assertEquals("END", readLine(in));

            sendDelete(out, "alpha");
            assertEquals("DELETED", readLine(in));

            sendGet(out, "alpha");
            assertEquals("END", readLine(in));
        }
    }

    @Test
    void getsAndCasFlow() throws Exception {
        try (Socket socket = connect();
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            flushAll(out, in);

            sendStorage(out, "set", "item", "one", 0, 0);
            assertEquals("STORED", readLine(in));

            sendGets(out, "item");
            String header = readLine(in);
            long firstCas = parseCas(header);
            assertEquals("one", readLine(in));
            assertEquals("END", readLine(in));

            sendCas(out, "item", "two", 0, 0, firstCas);
            assertEquals("STORED", readLine(in));

            sendGets(out, "item");
            long secondCas = parseCas(readLine(in));
            assertTrue(secondCas != firstCas);
            assertEquals("two", readLine(in));
            assertEquals("END", readLine(in));

            sendCas(out, "item", "three", 0, 0, firstCas);
            assertEquals("EXISTS", readLine(in));
        }
    }

    @Test
    void storageVariants() throws Exception {
        try (Socket socket = connect();
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            flushAll(out, in);

            sendStorage(out, "add", "item", "A", 7, 0);
            assertEquals("STORED", readLine(in));

            sendStorage(out, "add", "item", "B", 7, 0);
            assertEquals("NOT_STORED", readLine(in));

            sendStorage(out, "replace", "missing", "C", 9, 0);
            assertEquals("NOT_STORED", readLine(in));

            sendStorage(out, "replace", "item", "B", 9, 0);
            assertEquals("STORED", readLine(in));

            sendStorage(out, "append", "item", "C", 0, 0);
            assertEquals("STORED", readLine(in));

            sendStorage(out, "prepend", "item", "Z", 0, 0);
            assertEquals("STORED", readLine(in));

            sendGet(out, "item");
            assertEquals("VALUE item 9 3", readLine(in));
            assertEquals("ZBC", readLine(in));
            assertEquals("END", readLine(in));
        }
    }

    @Test
    void incrementAndDecrement() throws Exception {
        try (Socket socket = connect();
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            flushAll(out, in);

            sendStorage(out, "set", "num", "10", 0, 0);
            assertEquals("STORED", readLine(in));

            sendArithmetic(out, "incr", "num", 5);
            assertEquals("15", readLine(in));

            sendArithmetic(out, "decr", "num", 20);
            assertEquals("0", readLine(in));

            sendStorage(out, "set", "text", "abc", 0, 0);
            assertEquals("STORED", readLine(in));

            sendArithmetic(out, "incr", "text", 1);
            assertEquals("CLIENT_ERROR cannot increment or decrement non-numeric value", readLine(in));
        }
    }

    @Test
    void touchExtendsExpiration() throws Exception {
        try (Socket socket = connect();
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            flushAll(out, in);

            sendStorage(out, "set", "ttl", "v", 0, 1);
            assertEquals("STORED", readLine(in));

            Thread.sleep(400);
            sendTouch(out, "ttl", 5);
            assertEquals("TOUCHED", readLine(in));

            Thread.sleep(1500);
            sendGet(out, "ttl");
            assertEquals("VALUE ttl 0 1", readLine(in));
            assertEquals("v", readLine(in));
            assertEquals("END", readLine(in));

            Thread.sleep(4000);
            sendGet(out, "ttl");
            assertEquals("END", readLine(in));
        }
    }

    @Test
    void flushAllImmediateAndDelayed() throws Exception {
        try (Socket socket = connect();
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            flushAll(out, in);

            sendStorage(out, "set", "k1", "v1", 0, 0);
            assertEquals("STORED", readLine(in));
            sendStorage(out, "set", "k2", "v2", 0, 0);
            assertEquals("STORED", readLine(in));

            sendFlush(out, 0, false);
            assertEquals("OK", readLine(in));

            sendGet(out, "k1");
            assertEquals("END", readLine(in));

            sendStorage(out, "set", "k3", "v3", 0, 0);
            assertEquals("STORED", readLine(in));

            sendFlush(out, 1, false);
            assertEquals("OK", readLine(in));

            sendGet(out, "k3");
            assertEquals("VALUE k3 0 2", readLine(in));
            assertEquals("v3", readLine(in));
            assertEquals("END", readLine(in));

            Thread.sleep(1200);
            sendGet(out, "k3");
            assertEquals("END", readLine(in));
        }
    }

    @Test
    void statsCommandReportsBasicMetrics() throws Exception {
        try (Socket socket = connect();
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            flushAll(out, in);

            sendStats(out);
            Map<String, String> stats = readStats(in);
            assertTrue(stats.containsKey("version"));
            assertEquals("0", stats.get("curr_items"));
            assertTrue(stats.containsKey("cmd_get"));
            assertTrue(stats.containsKey("cmd_set"));
        }
    }

    private Socket connect() throws IOException {
        return new Socket("127.0.0.1", server.port());
    }

    private void sendStorage(BufferedOutputStream out, String command, String key, String value, int flags, int ttlSeconds) throws IOException {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        String header = String.format("%s %s %d %d %d\r\n", command, key, flags, ttlSeconds, data.length);
        out.write(header.getBytes(StandardCharsets.US_ASCII));
        out.write(data);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private void sendCas(BufferedOutputStream out, String key, String value, int flags, int ttlSeconds, long cas) throws IOException {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        String header = String.format("cas %s %d %d %d %d\r\n", key, flags, ttlSeconds, data.length, cas);
        out.write(header.getBytes(StandardCharsets.US_ASCII));
        out.write(data);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private void sendGet(BufferedOutputStream out, String key) throws IOException {
        out.write(("get " + key + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private void sendGets(BufferedOutputStream out, String key) throws IOException {
        out.write(("gets " + key + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private void sendDelete(BufferedOutputStream out, String key) throws IOException {
        out.write(("delete " + key + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private void sendArithmetic(BufferedOutputStream out, String command, String key, long delta) throws IOException {
        out.write(String.format("%s %s %d\r\n", command, key, delta).getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private void sendTouch(BufferedOutputStream out, String key, int ttlSeconds) throws IOException {
        out.write(String.format("touch %s %d\r\n", key, ttlSeconds).getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private void sendFlush(BufferedOutputStream out, int delaySeconds, boolean noreply) throws IOException {
        StringBuilder builder = new StringBuilder("flush_all");
        if (delaySeconds > 0) {
            builder.append(' ').append(delaySeconds);
        }
        if (noreply) {
            builder.append(" noreply");
        }
        builder.append("\r\n");
        out.write(builder.toString().getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private void sendStats(BufferedOutputStream out) throws IOException {
        out.write("stats\r\n".getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private void flushAll(BufferedOutputStream out, BufferedInputStream in) throws IOException {
        sendFlush(out, 0, false);
        assertEquals("OK", readLine(in));
    }

    private Map<String, String> readStats(BufferedInputStream in) throws IOException {
        Map<String, String> stats = new HashMap<>();
        String line;
        while ((line = readLine(in)) != null) {
            if ("END".equals(line)) {
                break;
            }
            String[] parts = line.split(" ", 3);
            if (parts.length == 3) {
                stats.put(parts[1], parts[2]);
            }
        }
        return stats;
    }

    private long parseCas(String header) {
        String[] parts = header.split(" ");
        return Long.parseLong(parts[4]);
    }

    private String readLine(BufferedInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = in.read()) != -1) {
            if (ch == '\n') {
                int len = sb.length();
                if (len > 0 && sb.charAt(len - 1) == '\r') {
                    sb.setLength(len - 1);
                }
                return sb.toString();
            }
            sb.append((char) ch);
        }
        return sb.length() == 0 ? null : sb.toString();
    }
}
