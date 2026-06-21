package com.cancache.agent.integration;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

final class ReplicationTestClient implements Closeable
{
    private static final byte CMD_SET = 'S';
    private static final byte CMD_GET = 'G';
    private static final byte RESP_HIT = 'H';
    private static final byte RESP_MISS = 'M';
    private static final byte RESP_TRUE = 'T';
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(3);

    private final Socket socket;
    private final DataInputStream input;
    private final DataOutputStream output;

    private ReplicationTestClient(Socket socket) throws IOException
    {
        this.socket = socket;
        this.input = new DataInputStream(socket.getInputStream());
        this.output = new DataOutputStream(socket.getOutputStream());
    }

    static ReplicationTestClient connect(String host, int port) throws IOException
    {
        Socket socket = new Socket();
        int timeoutMillis = Math.toIntExact(CONNECT_TIMEOUT.toMillis());
        socket.setSoTimeout(timeoutMillis);
        socket.connect(new InetSocketAddress(host, port), timeoutMillis);
        return new ReplicationTestClient(socket);
    }

    static Endpoint endpoint(String hostEnv, String portEnv, String fallbackHost, int fallbackPort)
    {
        String host = Optional.ofNullable(System.getenv(hostEnv))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .orElse(fallbackHost);
        int port = Optional.ofNullable(System.getenv(portEnv))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .map(Integer::parseInt)
                .orElse(fallbackPort);
        return new Endpoint(host, port);
    }

    boolean set(String key, String value, long expireAtMillis) throws IOException
    {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeByte(CMD_SET);
        output.writeInt(keyBytes.length);
        output.writeInt(valueBytes.length);
        output.writeLong(expireAtMillis);
        output.write(keyBytes);
        output.write(valueBytes);
        output.flush();
        return input.readByte() == RESP_TRUE;
    }

    Optional<String> get(String key) throws IOException
    {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        output.writeByte(CMD_GET);
        output.writeInt(keyBytes.length);
        output.write(keyBytes);
        output.flush();

        byte response = input.readByte();
        if (response == RESP_MISS) {
            return Optional.empty();
        }
        if (response != RESP_HIT) {
            throw new EOFException("Unexpected replication get response: " + response);
        }
        int valueLength = input.readInt();
        byte[] valueBytes = input.readNBytes(valueLength);
        if (valueBytes.length != valueLength) {
            throw new EOFException("Expected " + valueLength + " bytes but received " + valueBytes.length);
        }
        return Optional.of(new String(valueBytes, StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException
    {
        socket.close();
    }

    record Endpoint(String host, int port)
    {
    }
}
