package com.can.cluster.coordination;

import com.can.cluster.ClusterState;
import com.can.config.AppProperties;
import com.can.core.CacheEngine;
import io.vertx.core.WorkerExecutor;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
/**
 * Diğer düğümlerden gelen replikasyon komutlarını kabul ederek {@link CacheEngine}
 * üzerinde doğrudan uygulayan hafif TCP sunucusudur. Protokol, {@link RemoteNode}
 * tarafından kullanılan tek baytlık komutlardan oluşur ve bloklanmayı önlemek için
 * istek başına sanal thread'lerle çalışır.
 */
@Singleton
@Startup
public class ReplicationServer implements AutoCloseable
{
    private static final Logger LOG = Logger.getLogger(ReplicationServer.class);

    private final CacheEngine<String, String> engine;
    private final AppProperties.Replication config;
    private final ClusterState clusterState;
    private final WorkerExecutor workerExecutor;

    private volatile boolean running;
    private ServerSocket serverSocket;
    private Thread acceptThread;

    @Inject
    public ReplicationServer(CacheEngine<String, String> engine,
                             ClusterState clusterState,
                             AppProperties properties,
                             WorkerExecutor workerExecutor) {
        this.engine = engine;
        this.clusterState = clusterState;
        this.config = properties.cluster().replication();
        this.workerExecutor = workerExecutor;
    }

    @PostConstruct
    void start() {
        try {
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(config.bindHost(), config.port()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to bind replication port", e);
        }

        running = true;
        acceptThread = new Thread(this::acceptLoop, "replication-acceptor");
        acceptThread.setDaemon(true);
        acceptThread.start();
        LOG.infof("Replication server listening on %s:%d (advertised as %s:%d)",
                config.bindHost(), serverSocket.getLocalPort(), config.advertiseHost(), config.port());
    }

    private void acceptLoop()
    {
        while (running)
        {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                workerExecutor.executeBlocking(promise -> {
                    try {
                        handleClient(socket);
                        promise.complete();
                    } catch (Exception e) {
                        promise.fail(e);
                    }
                }, false);
            } catch (SocketException e) {
                if (running) {
                    LOG.warn("Replication server socket closed unexpectedly", e);
                }
                break;
            } catch (IOException e) {
                if (running) {
                    LOG.error("Failed to accept replication client", e);
                }
            }
        }
    }

    private void handleClient(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

            while (running && !socket.isClosed()) {
                byte command;
                try {
                    command = in.readByte();
                } catch (EOFException eof) {
                    break;
                }

                switch (command) {
                    case 'S' -> handleSet(in, out);
                    case 'G' -> handleGet(in, out);
                    case 'D' -> handleDelete(in, out);
                    case 'C' -> handleClear(out);
                    case 'X' -> handleCas(in, out);
                    case 'J' -> handleJoin(in, out);
                    case 'R' -> handleStream(out);
                    case 'H' -> handleDigest(out);
                    default -> {
                        LOG.warnf("Unknown replication command %d from %s", command & 0xff, socket.getRemoteSocketAddress());
                        return;
                    }
                }
            }
        } catch (IOException e) {
            LOG.debugf(e, "Replication client %s disconnected with error", socket.getRemoteSocketAddress());
        }
    }

    private void handleSet(DataInputStream in, DataOutputStream out) throws IOException {
        int keyLen = in.readInt();
        int valueLen = in.readInt();
        long expireAt = in.readLong();

        byte[] keyBytes = in.readNBytes(keyLen);
        byte[] valueBytes = in.readNBytes(valueLen);
        if (keyBytes.length != keyLen || valueBytes.length != valueLen) {
            throw new EOFException("Incomplete replication payload");
        }

        String key = new String(keyBytes, StandardCharsets.UTF_8);
        String value = new String(valueBytes, StandardCharsets.UTF_8);
        long now = System.currentTimeMillis();

        boolean stored;
        if (expireAt <= 0L) {
            stored = engine.set(key, value);
        } else if (expireAt <= now) {
            engine.delete(key);
            stored = true;
        } else {
            long ttlMillis = expireAt - now;
            stored = engine.set(key, value, Duration.ofMillis(ttlMillis));
        }

        out.writeByte(stored ? 'T' : 'F');
        out.flush();
    }

    private void handleGet(DataInputStream in, DataOutputStream out) throws IOException
    {
        int keyLen = in.readInt();
        byte[] keyBytes = in.readNBytes(keyLen);
        if (keyBytes.length != keyLen) {
            throw new EOFException("Incomplete get payload");
        }

        String key = new String(keyBytes, StandardCharsets.UTF_8);
        String value = engine.get(key);

        if (value == null) {
            out.writeByte('M');
            out.flush();
            return;
        }

        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeByte('H');
        out.writeInt(valueBytes.length);
        out.write(valueBytes);
        out.flush();
    }

    private void handleDelete(DataInputStream in, DataOutputStream out) throws IOException {
        int keyLen = in.readInt();
        byte[] keyBytes = in.readNBytes(keyLen);
        if (keyBytes.length != keyLen) throw new EOFException("Incomplete delete payload");

        String key = new String(keyBytes, StandardCharsets.UTF_8);
        boolean removed = engine.delete(key);
        out.writeByte(removed ? 'T' : 'F');
        out.flush();
    }

    private void handleClear(DataOutputStream out) throws IOException
    {
        engine.clear();
        out.writeByte('O');
        out.flush();
    }

    private void handleCas(DataInputStream in, DataOutputStream out) throws IOException {
        int keyLen = in.readInt();
        int valueLen = in.readInt();
        long expireAt = in.readLong();
        long expectedCas = in.readLong();

        byte[] keyBytes = in.readNBytes(keyLen);
        byte[] valueBytes = in.readNBytes(valueLen);
        if (keyBytes.length != keyLen || valueBytes.length != valueLen) {
            throw new EOFException("Incomplete cas payload");
        }

        String key = new String(keyBytes, StandardCharsets.UTF_8);
        String value = new String(valueBytes, StandardCharsets.UTF_8);
        long now = System.currentTimeMillis();

        boolean stored;
        if (expireAt <= 0L) {
            stored = engine.compareAndSwap(key, value, expectedCas, null);
        } else if (expireAt <= now) {
            engine.delete(key);
            stored = true;
        } else {
            long ttlMillis = expireAt - now;
            stored = engine.compareAndSwap(key, value, expectedCas, Duration.ofMillis(ttlMillis));
        }

        out.writeByte(stored ? 'T' : 'F');
        out.flush();
    }

    private void handleJoin(DataInputStream in, DataOutputStream out) throws IOException
    {
        int joinerIdLength = in.readInt();
        byte[] joinerIdBytes = in.readNBytes(joinerIdLength);
        if (joinerIdBytes.length != joinerIdLength) {
            throw new EOFException("Incomplete join request");
        }
        long joinerEpoch = in.readLong();
        clusterState.observeEpoch(joinerEpoch);

        String joinerId = new String(joinerIdBytes, StandardCharsets.UTF_8);
        if (Objects.equals(joinerId, clusterState.localNodeId())) {
            out.writeByte('R');
            out.flush();
            return;
        }

        byte[] idBytes = clusterState.localNodeIdBytes();
        out.writeByte('A');
        out.writeInt(idBytes.length);
        out.write(idBytes);
        out.writeLong(clusterState.currentEpoch());
        out.flush();
    }

    private void handleStream(DataOutputStream out) throws IOException
    {
        try {
            engine.forEachEntry((key, value, expireAt) -> {
                try {
                    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                    out.writeByte(1);
                    out.writeInt(keyBytes.length);
                    out.writeInt(value.length);
                    out.writeLong(expireAt);
                    out.write(keyBytes);
                    out.write(value);
                } catch (IOException e) {
                    throw new StreamWriteException(e);
                }
            });
        } catch (StreamWriteException e) {
            throw e.unwrap();
        }
        out.writeByte(0);
        out.flush();
    }

    private void handleDigest(DataOutputStream out) throws IOException
    {
        out.writeLong(engine.fingerprint());
        out.flush();
    }

    @PreDestroy
    @Override
    public void close() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
        if (acceptThread != null) {
            acceptThread.interrupt();
        }
    }
    private static final class StreamWriteException extends RuntimeException
    {
        private final IOException cause;

        StreamWriteException(IOException cause) {
            this.cause = cause;
        }

        IOException unwrap() {
            return cause;
        }
    }
}
