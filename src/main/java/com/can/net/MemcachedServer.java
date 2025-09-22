package com.can.net;

import com.can.cluster.ClusterClient;
import com.can.config.AppProperties;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Memcached text protokolünü taklit eden basit bir TCP sunucusudur. Quarkus uygulaması
 * ayağa kalktığında belirtilen port üzerinden bağlantıları kabul eder ve gelen komutları
 * {@link ClusterClient} aracılığıyla küme içindeki düğümlere yönlendirir.
 */
@Startup
@Singleton
public class MemcachedServer implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(MemcachedServer.class);
    private static final byte[] CRLF = new byte[]{'\r', '\n'};
    private static final long THIRTY_DAYS_SECONDS = 60L * 60L * 24L * 30L;

    private final ClusterClient<String, String> clusterClient;
    private final AppProperties.Network networkConfig;

    private volatile boolean running;
    private ThreadPoolExecutor workers;
    private ServerSocket serverSocket;
    private Thread acceptThread;

    @Inject
    public MemcachedServer(ClusterClient<String, String> clusterClient, AppProperties properties) {
        this.clusterClient = Objects.requireNonNull(clusterClient, "clusterClient");
        this.networkConfig = Objects.requireNonNull(properties.network(), "networkConfig");
    }

    @PostConstruct
    void start() {
        int workerThreads = Math.max(1, networkConfig.workerThreads());
        workers = new ThreadPoolExecutor(
                workerThreads,
                workerThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("memcached-worker-"));

        try {
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(networkConfig.host(), networkConfig.port()),
                    Math.max(1, networkConfig.backlog()));
        } catch (IOException e) {
            workers.shutdownNow();
            throw new IllegalStateException("Failed to bind memcached port", e);
        }

        running = true;
        acceptThread = new Thread(this::acceptLoop, "memcached-acceptor");
        acceptThread.setDaemon(true);
        acceptThread.start();
        LOG.infof("Memcached-compatible server listening on %s:%d", networkConfig.host(), serverSocket.getLocalPort());
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                client.setTcpNoDelay(true);
                workers.execute(() -> handleClient(client));
            } catch (SocketException e) {
                if (running) {
                    LOG.warn("Socket closed unexpectedly while accepting clients", e);
                }
                break;
            } catch (IOException e) {
                if (running) {
                    LOG.error("Failed to accept client connection", e);
                }
            }
        }
    }

    private void handleClient(Socket socket) {
        try (socket;
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            while (running && !socket.isClosed()) {
                String line = readLine(in);
                if (line == null) {
                    break;
                }
                if (line.isEmpty()) {
                    continue;
                }
                if (!processCommand(line, in, out)) {
                    break;
                }
            }
        } catch (IOException e) {
            LOG.debugf(e, "Client %s disconnected with error", socket.getRemoteSocketAddress());
        }
    }

    private boolean processCommand(String line, BufferedInputStream in, BufferedOutputStream out) throws IOException {
        String[] parts = line.trim().split("\\s+");
        if (parts.length == 0) {
            return true;
        }
        String command = parts[0].toLowerCase(Locale.ROOT);
        return switch (command) {
            case "set" -> handleSet(parts, in, out);
            case "get", "gets" -> {
                handleGet(parts, out);
                yield true;
            }
            case "delete" -> handleDelete(parts, out);
            case "version" -> {
                writeLine(out, "VERSION " + getVersion());
                out.flush();
                yield true;
            }
            case "quit" -> false;
            default -> {
                writeLine(out, "ERROR");
                out.flush();
                yield true;
            }
        };
    }

    private boolean handleSet(String[] parts, BufferedInputStream in, BufferedOutputStream out) throws IOException {
        if (parts.length < 5) {
            writeLine(out, "CLIENT_ERROR bad command line format");
            out.flush();
            return true;
        }
        boolean noreply = parts.length == 6 && "noreply".equalsIgnoreCase(parts[5]);
        if (parts.length > 6 || (parts.length == 6 && !noreply)) {
            writeLine(out, "CLIENT_ERROR invalid arguments");
            out.flush();
            return true;
        }

        String key = parts[1];
        long bytes;
        long exptime;
        try {
            // flags param is parts[2] - intentionally ignored but validated
            Long.parseLong(parts[2]);
            exptime = Long.parseLong(parts[3]);
            bytes = Long.parseLong(parts[4]);
        } catch (NumberFormatException e) {
            writeLine(out, "CLIENT_ERROR numeric value expected");
            out.flush();
            return true;
        }

        if (bytes < 0 || bytes > Integer.MAX_VALUE) {
            writeLine(out, "CLIENT_ERROR bad data chunk");
            out.flush();
            return true;
        }

        int length = (int) bytes;
        byte[] payload = in.readNBytes(length + CRLF.length);
        if (payload.length < length + CRLF.length) {
            writeLine(out, "CLIENT_ERROR bad data chunk");
            out.flush();
            return true;
        }
        if (payload[length] != '\r' || payload[length + 1] != '\n') {
            writeLine(out, "CLIENT_ERROR bad data chunk");
            out.flush();
            return true;
        }

        String value = new String(payload, 0, length, StandardCharsets.UTF_8);
        Duration ttl = parseExpiration(exptime);
        if (Duration.ZERO.equals(ttl)) {
            clusterClient.delete(key);
        } else {
            clusterClient.set(key, value, ttl);
        }

        if (!noreply) {
            writeLine(out, "STORED");
            out.flush();
        }
        return true;
    }

    private void handleGet(String[] parts, BufferedOutputStream out) throws IOException {
        if (parts.length < 2) {
            writeLine(out, "CLIENT_ERROR bad command line format");
            out.flush();
            return;
        }
        for (int i = 1; i < parts.length; i++) {
            String key = parts[i];
            String value = clusterClient.get(key);
            if (value != null) {
                byte[] data = value.getBytes(StandardCharsets.UTF_8);
                writeLine(out, "VALUE " + key + " 0 " + data.length);
                out.write(data);
                out.write(CRLF);
            }
        }
        writeLine(out, "END");
        out.flush();
    }

    private boolean handleDelete(String[] parts, BufferedOutputStream out) throws IOException {
        if (parts.length < 2) {
            writeLine(out, "CLIENT_ERROR bad command line format");
            out.flush();
            return true;
        }
        boolean noreply = parts.length == 3 && "noreply".equalsIgnoreCase(parts[2]);
        if (parts.length > 3 || (parts.length == 3 && !noreply)) {
            writeLine(out, "CLIENT_ERROR invalid arguments");
            out.flush();
            return true;
        }
        boolean removed = clusterClient.delete(parts[1]);
        if (!noreply) {
            writeLine(out, removed ? "DELETED" : "NOT_FOUND");
            out.flush();
        }
        return true;
    }

    private Duration parseExpiration(long exptime) {
        if (exptime <= 0) {
            return null;
        }
        if (exptime > THIRTY_DAYS_SECONDS) {
            long now = Instant.now().getEpochSecond();
            long delta = exptime - now;
            if (delta <= 0) {
                return Duration.ZERO;
            }
            return Duration.ofSeconds(delta);
        }
        return Duration.ofSeconds(exptime);
    }

    private String readLine(InputStream in) throws IOException {
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

    private void writeLine(BufferedOutputStream out, String line) throws IOException {
        out.write(line.getBytes(StandardCharsets.US_ASCII));
        out.write(CRLF);
    }

    private String getVersion() {
        Package pkg = MemcachedServer.class.getPackage();
        if (pkg != null && pkg.getImplementationVersion() != null) {
            return pkg.getImplementationVersion();
        }
        return "0.0.1";
    }

    /**
     * Sunucunun dinlediği portu testler için elde etmek adına yayınlanır.
     */
    public int port() {
        return serverSocket != null ? serverSocket.getLocalPort() : networkConfig.port();
    }

    @PreDestroy
    @Override
    public void close() {
        running = false;
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                LOG.debug("Failed to close server socket", e);
            }
        }
        if (acceptThread != null) {
            try {
                acceptThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (workers != null) {
            workers.shutdownNow();
            try {
                workers.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(1);

        private NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }
}
