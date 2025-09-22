package com.can.cluster.coordination;

import com.can.cluster.Node;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Bir başka can-cache örneğinin replikasyon sunucusuna TCP üzerinden bağlanarak
 * {@link Node} sözleşmesini uygulayan hafif bir vekil düğümdür. Yazma işlemleri
 * hedef düğümün bellek motoruna doğrudan iletilir, okuma ve silme çağrıları ise
 * aynı protokol üzerinden yanıtlanır. Her çağrı için kısa ömürlü bir soket
 * açıldığı için istemciler arasında durum paylaşımı yapılmaz ve başarısız
 * bağlantılar yukarıya istisna olarak fırlatılır.
 */
public final class RemoteNode implements Node<String, String>
{
    private static final byte CMD_SET = 'S';
    private static final byte CMD_GET = 'G';
    private static final byte CMD_DELETE = 'D';
    private static final byte RESP_OK = 'O';
    private static final byte RESP_HIT = 'H';
    private static final byte RESP_MISS = 'M';
    private static final byte RESP_TRUE = 'T';
    private static final byte RESP_FALSE = 'F';

    private final String id;
    private final InetSocketAddress address;
    private final int connectTimeoutMillis;

    public RemoteNode(String id, String host, int port, int connectTimeoutMillis) {
        this.id = id;
        this.address = new InetSocketAddress(host, port);
        this.connectTimeoutMillis = Math.max(100, connectTimeoutMillis);
    }

    @Override
    public void set(String key, String value, Duration ttl) {
        long expireAt = ttl == null || ttl.isZero() || ttl.isNegative()
                ? 0L
                : System.currentTimeMillis() + ttl.toMillis();

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        execute(socket -> {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            out.writeByte(CMD_SET);
            out.writeInt(keyBytes.length);
            out.writeInt(valueBytes.length);
            out.writeLong(expireAt);
            out.write(keyBytes);
            out.write(valueBytes);
            out.flush();

            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            byte response = in.readByte();
            if (response != RESP_OK) {
                throw new IOException("unexpected response to set: " + (char) response);
            }
        });
    }

    @Override
    public String get(String key) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        return execute(socket -> {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            out.writeByte(CMD_GET);
            out.writeInt(keyBytes.length);
            out.write(keyBytes);
            out.flush();

            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            byte response = in.readByte();
            if (response == RESP_MISS) {
                return null;
            }
            if (response != RESP_HIT) {
                throw new IOException("unexpected response to get: " + (char) response);
            }
            int valueLength = in.readInt();
            byte[] valueBytes = in.readNBytes(valueLength);
            if (valueBytes.length != valueLength) {
                throw new EOFException("incomplete value payload");
            }
            return new String(valueBytes, StandardCharsets.UTF_8);
        });
    }

    @Override
    public boolean delete(String key) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        return execute(socket -> {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            out.writeByte(CMD_DELETE);
            out.writeInt(keyBytes.length);
            out.write(keyBytes);
            out.flush();

            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            byte response = in.readByte();
            if (response == RESP_TRUE) {
                return true;
            }
            if (response == RESP_FALSE) {
                return false;
            }
            throw new IOException("unexpected response to delete: " + (char) response);
        });
    }

    @Override
    public String id() {
        return id;
    }

    private <T> T execute(RemoteCall<T> call) {
        try (Socket socket = new Socket()) {
            socket.connect(address, connectTimeoutMillis);
            socket.setTcpNoDelay(true);
            return call.execute(socket);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to communicate with node " + id + " at " + address, e);
        }
    }

    private void execute(RemoteCallWithoutResult call) {
        execute(socket -> {
            call.execute(socket);
            return null;
        });
    }

    @FunctionalInterface
    private interface RemoteCall<T> {
        T execute(Socket socket) throws IOException;
    }

    @FunctionalInterface
    private interface RemoteCallWithoutResult {
        void execute(Socket socket) throws IOException;
    }
}
