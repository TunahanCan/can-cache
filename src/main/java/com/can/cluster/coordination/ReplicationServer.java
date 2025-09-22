package com.can.cluster.coordination;

import com.can.config.AppProperties;
import com.can.core.CacheEngine;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private final AppProperties.Cluster.Replication config;

    private volatile boolean running;
    private ServerSocket serverSocket;
    private ExecutorService workers;
    private Thread acceptThread;

    @Inject
    public ReplicationServer(CacheEngine<String, String> engine, AppProperties properties) {
        this.engine = engine;
        this.config = properties.cluster().replication();
    }

    @PostConstruct
    void start() {
        workers = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
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

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                workers.execute(() -> handleClient(socket));
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

        if (expireAt <= 0L) {
            engine.set(key, value);
        } else if (expireAt <= now) {
            engine.delete(key);
        } else {
            long ttlMillis = expireAt - now;
            engine.set(key, value, Duration.ofMillis(ttlMillis));
        }

        out.writeByte('O');
        out.flush();
    }

    private void handleGet(DataInputStream in, DataOutputStream out) throws IOException {
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
        if (keyBytes.length != keyLen) {
            throw new EOFException("Incomplete delete payload");
        }

        String key = new String(keyBytes, StandardCharsets.UTF_8);
        boolean removed = engine.delete(key);
        out.writeByte(removed ? 'T' : 'F');
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
        if (workers != null) {
            workers.shutdownNow();
        }
    }
}
