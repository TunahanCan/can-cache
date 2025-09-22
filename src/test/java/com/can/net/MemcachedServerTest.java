package com.can.net;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
class MemcachedServerTest {

    @Inject
    MemcachedServer server;

    @Test
    void setGetDeleteCycle() throws Exception {
        assertNotNull(server);
        try (Socket socket = connect();
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            sendSet(out, "alpha", "value", 0);
            assertEquals("STORED", readLine(in));

            sendGet(out, "alpha");
            assertEquals("VALUE alpha 0 5", readLine(in));
            assertEquals("value", readLine(in));
            assertEquals("END", readLine(in));

            sendDelete(out, "alpha");
            assertEquals("DELETED", readLine(in));

            sendGet(out, "alpha");
            assertEquals("END", readLine(in));
        }
    }

    @Test
    void expirationEvictsEntry() throws Exception {
        try (Socket socket = connect();
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            sendSet(out, "ttl", "v", 1);
            assertEquals("STORED", readLine(in));

            Thread.sleep(1200L);

            sendGet(out, "ttl");
            assertEquals("END", readLine(in));
        }
    }

    private Socket connect() throws IOException {
        return new Socket("127.0.0.1", server.port());
    }

    private void sendSet(BufferedOutputStream out, String key, String value, int ttl) throws IOException {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        out.write(("set " + key + " 0 " + ttl + " " + data.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(data);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private void sendGet(BufferedOutputStream out, String key) throws IOException {
        out.write(("get " + key + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private void sendDelete(BufferedOutputStream out, String key) throws IOException {
        out.write(("delete " + key + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.flush();
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
