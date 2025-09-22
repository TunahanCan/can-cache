package com.can.net;

import com.can.cluster.ClusterClient;
import com.can.config.AppProperties;
import com.can.core.CacheEngine;
import com.can.core.StoredValueCodec;
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
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * cancached text protokolünü taklit eden basit bir TCP sunucusudur. Quarkus uygulaması
 * ayağa kalktığında belirtilen port üzerinden bağlantıları kabul eder ve gelen komutları
 * {@link ClusterClient} aracılığıyla küme içindeki düğümlere yönlendirir.
 */
@Startup
@Singleton
public class CanCachedServer implements AutoCloseable
{

    private static final Logger LOG = Logger.getLogger(CanCachedServer.class);
    private static final byte[] CRLF = new byte[]{'\r', '\n'};
    private static final long THIRTY_DAYS_SECONDS = 60L * 60L * 24L * 30L;
    private static final int MAX_ITEM_SIZE = 1_048_576; // 1 MB
    private static final int MAX_CAS_RETRIES = 16;

    private final ClusterClient clusterClient;
    private final AppProperties.Network networkConfig;
    private final CacheEngine<String, String> localEngine;

    private final AtomicLong casCounter = new AtomicLong(1L);
    private final AtomicLong cmdGet = new AtomicLong();
    private final AtomicLong cmdSet = new AtomicLong();
    private final AtomicLong cmdTouch = new AtomicLong();
    private final AtomicLong cmdFlush = new AtomicLong();
    private final AtomicLong getHits = new AtomicLong();
    private final AtomicLong getMisses = new AtomicLong();
    private final AtomicLong totalItems = new AtomicLong();
    private final AtomicLong currItems = new AtomicLong();
    private final AtomicLong currConnections = new AtomicLong();
    private final AtomicLong totalConnections = new AtomicLong();
    private final AtomicLong flushDeadlineMillis = new AtomicLong(0L);
    private final long startTime = System.currentTimeMillis();

    private volatile boolean running;
    private ThreadPoolExecutor workers;
    private ServerSocket serverSocket;
    private Thread acceptThread;
    private AutoCloseable removalSubscription;

    @Inject
    public CanCachedServer(ClusterClient clusterClient, AppProperties properties, CacheEngine<String, String> localEngine) {
        this.clusterClient = Objects.requireNonNull(clusterClient, "clusterClient");
        this.networkConfig = Objects.requireNonNull(properties.network(), "networkConfig");
        this.localEngine = Objects.requireNonNull(localEngine, "localEngine");
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
                new NamedThreadFactory("cancached-worker-"));

        try {
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(networkConfig.host(), networkConfig.port()),
                    Math.max(1, networkConfig.backlog()));
        } catch (IOException e) {
            workers.shutdownNow();
            throw new IllegalStateException("Failed to bind cancached port", e);
        }

        running = true;
        acceptThread = new Thread(this::acceptLoop, "cancached-acceptor");
        acceptThread.setDaemon(true);
        acceptThread.start();
        LOG.infof("cancached-compatible server listening on %s:%d", networkConfig.host(), serverSocket.getLocalPort());
        removalSubscription = localEngine.onRemoval(key -> decrementCurrItems());
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
        currConnections.incrementAndGet();
        totalConnections.incrementAndGet();
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
        } finally {
            currConnections.decrementAndGet();
        }
    }

    private boolean processCommand(String line, BufferedInputStream in, BufferedOutputStream out) throws IOException
    {
        maybeApplyDelayedFlush();
        String[] parts = line.trim().split("\\s+");
        if (parts.length == 0) {
            return true;
        }
        String command = parts[0].toLowerCase(Locale.ROOT);
        return switch (command) {
            case "set", "add", "replace", "append", "prepend", "cas" -> handleStorageCommand(command, parts, in, out);
            case "get" -> {
                handleGet(parts, out, false);
                yield true;
            }
            case "gets" -> {
                handleGet(parts, out, true);
                yield true;
            }
            case "delete" -> handleDelete(parts, out);
            case "incr", "decr" -> handleIncrDecr(command, parts, out);
            case "touch" -> handleTouch(parts, out);
            case "flush_all" -> handleFlushAll(parts, out);
            case "stats" -> {
                handleStats(out);
                yield true;
            }
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

    private boolean handleStorageCommand(String command, String[] parts, BufferedInputStream in, BufferedOutputStream out) throws IOException {
        boolean isCas = "cas".equals(command);
        int minParts = isCas ? 6 : 5;
        if (parts.length < minParts) {
            writeLine(out, "CLIENT_ERROR bad command line format");
            out.flush();
            return true;
        }

        int flags;
        long exptime;
        long bytes;
        long casUnique = 0L;
        try {
            flags = Integer.parseInt(parts[2]);
            exptime = Long.parseLong(parts[3]);
            bytes = Long.parseLong(parts[4]);
            if (isCas) {
                casUnique = Long.parseUnsignedLong(parts[5]);
            }
        } catch (NumberFormatException e) {
            writeLine(out, "CLIENT_ERROR numeric value expected");
            out.flush();
            return true;
        }

        int noreplyIndex = isCas ? 6 : 5;
        boolean noreply = false;
        if (parts.length > noreplyIndex) {
            if (parts.length == noreplyIndex + 1 && "noreply".equalsIgnoreCase(parts[noreplyIndex])) {
                noreply = true;
            } else {
                writeLine(out, "CLIENT_ERROR invalid arguments");
                out.flush();
                return true;
            }
        }

        if (bytes < 0 || bytes > MAX_ITEM_SIZE) {
            writeLine(out, "CLIENT_ERROR bad data chunk");
            out.flush();
            return true;
        }

        byte[] payload = in.readNBytes((int) bytes + CRLF.length);
        if (payload.length < bytes + CRLF.length) {
            writeLine(out, "CLIENT_ERROR bad data chunk");
            out.flush();
            return true;
        }
        if (payload[(int) bytes] != '\r' || payload[(int) bytes + 1] != '\n') {
            writeLine(out, "CLIENT_ERROR bad data chunk");
            out.flush();
            return true;
        }

        byte[] value = Arrays.copyOf(payload, (int) bytes);
        String key = parts[1];
        Duration ttl = parseExpiration(exptime);
        cmdSet.incrementAndGet();

        StoredValueCodec.StoredValue existing = getEntry(key);
        if (existing != null && existing.expired(System.currentTimeMillis())) {
            existing = null;
        }

        if (Duration.ZERO.equals(ttl)) {
            if (existing != null && clusterClient.delete(key)) {
                decrementCurrItems();
            }
            if (!noreply) {
                writeLine(out, "STORED");
                out.flush();
            }
            return true;
        }

        if (isCas) {
            return handleCasCommand(key, value, flags, ttl, casUnique, noreply, existing, out);
        }

        switch (command) {
            case "add" -> {
                if (existing != null) {
                    if (!noreply) {
                        writeLine(out, "NOT_STORED");
                        out.flush();
                    }
                    return true;
                }
                StoredValueCodec.StoredValue entry = new StoredValueCodec.StoredValue(value, flags, nextCas(), computeExpireAt(ttl));
                if (!storeEntry(key, entry, ttl)) {
                    if (!noreply) {
                        writeLine(out, "NOT_STORED");
                        out.flush();
                    }
                    return true;
                }
                incrementItems();
            }
            case "replace" -> {
                if (existing == null) {
                    if (!noreply) {
                        writeLine(out, "NOT_STORED");
                        out.flush();
                    }
                    return true;
                }
                StoredValueCodec.StoredValue entry = new StoredValueCodec.StoredValue(value, flags, nextCas(), computeExpireAt(ttl));
                if (!storeEntry(key, entry, ttl)) {
                    if (!noreply) {
                        writeLine(out, "NOT_STORED");
                        out.flush();
                    }
                    return true;
                }
            }
            case "append" -> {
                if (existing == null) {
                    if (!noreply) {
                        writeLine(out, "NOT_STORED");
                        out.flush();
                    }
                    return true;
                }
                if ((long) existing.value().length + value.length > MAX_ITEM_SIZE) {
                    if (!noreply) {
                        writeLine(out, "SERVER_ERROR object too large");
                        out.flush();
                    }
                    return true;
                }
                CasUpdateStatus status = appendOrPrepend(key, existing, value, false);
                if (status == CasUpdateStatus.NOT_FOUND) {
                    if (!noreply) {
                        writeLine(out, "NOT_STORED");
                        out.flush();
                    }
                    return true;
                }
                if (status == CasUpdateStatus.TOO_LARGE) {
                    if (!noreply) {
                        writeLine(out, "SERVER_ERROR object too large");
                        out.flush();
                    }
                    return true;
                }
                if (status == CasUpdateStatus.CONFLICT) {
                    if (!noreply) {
                        writeLine(out, "SERVER_ERROR cas conflict");
                        out.flush();
                    }
                    return true;
                }
            }
            case "prepend" -> {
                if (existing == null) {
                    if (!noreply) {
                        writeLine(out, "NOT_STORED");
                        out.flush();
                    }
                    return true;
                }
                if ((long) existing.value().length + value.length > MAX_ITEM_SIZE) {
                    if (!noreply) {
                        writeLine(out, "SERVER_ERROR object too large");
                        out.flush();
                    }
                    return true;
                }
                CasUpdateStatus status = appendOrPrepend(key, existing, value, true);
                if (status == CasUpdateStatus.NOT_FOUND) {
                    if (!noreply) {
                        writeLine(out, "NOT_STORED");
                        out.flush();
                    }
                    return true;
                }
                if (status == CasUpdateStatus.TOO_LARGE) {
                    if (!noreply) {
                        writeLine(out, "SERVER_ERROR object too large");
                        out.flush();
                    }
                    return true;
                }
                if (status == CasUpdateStatus.CONFLICT) {
                    if (!noreply) {
                        writeLine(out, "SERVER_ERROR cas conflict");
                        out.flush();
                    }
                    return true;
                }
            }
            default -> {
                StoredValueCodec.StoredValue entry = new StoredValueCodec.StoredValue(value, flags, nextCas(), computeExpireAt(ttl));
                if (!storeEntry(key, entry, ttl)) {
                    if (!noreply) {
                        writeLine(out, "NOT_STORED");
                        out.flush();
                    }
                    return true;
                }
                if (existing == null) {
                    incrementItems();
                }
            }
        }

        if (!noreply) {
            writeLine(out, "STORED");
            out.flush();
        }
        return true;
    }

    private boolean handleCasCommand(String key, byte[] value, int flags, Duration ttl, long casUnique, boolean noreply, StoredValueCodec.StoredValue existing, BufferedOutputStream out) throws IOException {
        if (existing == null) {
            if (!noreply) {
                writeLine(out, "NOT_FOUND");
                out.flush();
            }
            return true;
        }
        if (existing.cas() != casUnique) {
            if (!noreply) {
                writeLine(out, "EXISTS");
                out.flush();
            }
            return true;
        }
        long expireAt = computeExpireAt(ttl);
        StoredValueCodec.StoredValue entry = new StoredValueCodec.StoredValue(value, flags, nextCas(), expireAt);
        Duration effectiveTtl = ttl;
        if (effectiveTtl == null) {
            effectiveTtl = ttlFromExpireAt(expireAt);
        }
        boolean stored = clusterClient.compareAndSwap(key, StoredValueCodec.encode(entry), casUnique, effectiveTtl);
        if (!stored) {
            StoredValueCodec.StoredValue latest = getEntry(key);
            if (!noreply) {
                if (latest == null) {
                    writeLine(out, "NOT_FOUND");
                } else {
                    writeLine(out, "EXISTS");
                }
                out.flush();
            }
            return true;
        }
        if (!noreply) {
            writeLine(out, "STORED");
            out.flush();
        }
        return true;
    }

    private CasUpdateStatus appendOrPrepend(String key, StoredValueCodec.StoredValue snapshot, byte[] addition, boolean prepend) {
        StoredValueCodec.StoredValue current = snapshot;
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            if (current == null) {
                return CasUpdateStatus.NOT_FOUND;
            }
            if ((long) current.value().length + addition.length > MAX_ITEM_SIZE) {
                return CasUpdateStatus.TOO_LARGE;
            }
            byte[] combined = new byte[current.value().length + addition.length];
            if (prepend) {
                System.arraycopy(addition, 0, combined, 0, addition.length);
                System.arraycopy(current.value(), 0, combined, addition.length, current.value().length);
            } else {
                System.arraycopy(current.value(), 0, combined, 0, current.value().length);
                System.arraycopy(addition, 0, combined, current.value().length, addition.length);
            }
            StoredValueCodec.StoredValue candidate = new StoredValueCodec.StoredValue(combined, current.flags(), nextCas(), current.expireAt());
            Duration ttl = ttlFromExpireAt(current.expireAt());
            if (clusterClient.compareAndSwap(key, StoredValueCodec.encode(candidate), current.cas(), ttl)) {
                return CasUpdateStatus.SUCCESS;
            }
            current = getEntry(key);
        }
        return CasUpdateStatus.CONFLICT;
    }

    private void handleGet(String[] parts, BufferedOutputStream out, boolean includeCas) throws IOException {
        if (parts.length < 2) {
            writeLine(out, "CLIENT_ERROR bad command line format");
            out.flush();
            return;
        }
        cmdGet.incrementAndGet();
        long now = System.currentTimeMillis();
        for (int i = 1; i < parts.length; i++) {
            String key = parts[i];
            StoredValueCodec.StoredValue entry = getEntry(key);
            if (entry == null || entry.expired(now)) {
                getMisses.incrementAndGet();
                continue;
            }
            getHits.incrementAndGet();
            String header = includeCas
                    ? String.format(Locale.ROOT, "VALUE %s %d %d %d", key, entry.flags(), entry.value().length, entry.cas())
                    : String.format(Locale.ROOT, "VALUE %s %d %d", key, entry.flags(), entry.value().length);
            writeLine(out, header);
            out.write(entry.value());
            out.write(CRLF);
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
        if (removed) {
            decrementCurrItems();
        }
        if (!noreply) {
            writeLine(out, removed ? "DELETED" : "NOT_FOUND");
            out.flush();
        }
        return true;
    }

    private boolean handleIncrDecr(String command, String[] parts, BufferedOutputStream out) throws IOException {
        if (parts.length < 3) {
            writeLine(out, "CLIENT_ERROR bad command line format");
            out.flush();
            return true;
        }
        boolean noreply = parts.length == 4 && "noreply".equalsIgnoreCase(parts[3]);
        if (parts.length > 4 || (parts.length == 4 && !noreply)) {
            writeLine(out, "CLIENT_ERROR invalid arguments");
            out.flush();
            return true;
        }
        BigInteger delta;
        try {
            delta = new BigInteger(parts[2]);
            if (delta.signum() < 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            writeLine(out, "CLIENT_ERROR invalid numeric delta");
            out.flush();
            return true;
        }

        String key = parts[1];
        StoredValueCodec.StoredValue current = getEntry(key);
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            if (current == null) {
                if (!noreply) {
                    writeLine(out, "NOT_FOUND");
                    out.flush();
                }
                return true;
            }
            String currentValue = new String(current.value(), StandardCharsets.US_ASCII);
            if (!currentValue.chars().allMatch(Character::isDigit)) {
                writeLine(out, "CLIENT_ERROR cannot increment or decrement non-numeric value");
                out.flush();
                return true;
            }
            BigInteger numeric = new BigInteger(currentValue);
            BigInteger updated = "incr".equals(command) ? numeric.add(delta) : numeric.subtract(delta);
            if (updated.signum() < 0) {
                updated = BigInteger.ZERO;
            }
            byte[] newValue = updated.toString().getBytes(StandardCharsets.US_ASCII);
            if (newValue.length > MAX_ITEM_SIZE) {
                writeLine(out, "SERVER_ERROR object too large");
                out.flush();
                return true;
            }
            StoredValueCodec.StoredValue candidate = new StoredValueCodec.StoredValue(newValue, current.flags(), nextCas(), current.expireAt());
            Duration ttl = ttlFromExpireAt(current.expireAt());
            if (clusterClient.compareAndSwap(key, StoredValueCodec.encode(candidate), current.cas(), ttl)) {
                if (!noreply) {
                    writeLine(out, updated.toString());
                    out.flush();
                }
                return true;
            }
            current = getEntry(key);
        }
        if (!noreply) {
            writeLine(out, "SERVER_ERROR cas conflict");
            out.flush();
        }
        return true;
    }
    private boolean handleTouch(String[] parts, BufferedOutputStream out) throws IOException {
        if (parts.length < 3) {
            writeLine(out, "CLIENT_ERROR bad command line format");
            out.flush();
            return true;
        }
        boolean noreply = parts.length == 4 && "noreply".equalsIgnoreCase(parts[3]);
        if (parts.length > 4 || (parts.length == 4 && !noreply)) {
            writeLine(out, "CLIENT_ERROR invalid arguments");
            out.flush();
            return true;
        }
        long exptime;
        try {
            exptime = Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            writeLine(out, "CLIENT_ERROR numeric value expected");
            out.flush();
            return true;
        }
        Duration ttl = parseExpiration(exptime);
        if (Duration.ZERO.equals(ttl)) {
            if (clusterClient.delete(parts[1])) {
                decrementCurrItems();
            }
            if (!noreply) {
                writeLine(out, "NOT_FOUND");
                out.flush();
            }
            return true;
        }
        StoredValueCodec.StoredValue current = getEntry(parts[1]);
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            if (current == null) {
                if (!noreply) {
                    writeLine(out, "NOT_FOUND");
                    out.flush();
                }
                return true;
            }
            long expireAt = computeExpireAt(ttl);
            StoredValueCodec.StoredValue candidate = new StoredValueCodec.StoredValue(current.value(), current.flags(), current.cas(), expireAt);
            Duration effectiveTtl = ttl;
            if (effectiveTtl == null) {
                effectiveTtl = ttlFromExpireAt(expireAt);
            }
            if (clusterClient.compareAndSwap(parts[1], StoredValueCodec.encode(candidate), current.cas(), effectiveTtl)) {
                cmdTouch.incrementAndGet();
                if (!noreply) {
                    writeLine(out, "TOUCHED");
                    out.flush();
                }
                return true;
            }
            current = getEntry(parts[1]);
        }
        if (!noreply) {
            writeLine(out, "SERVER_ERROR cas conflict");
            out.flush();
        }
        return true;
    }
    private boolean handleFlushAll(String[] parts, BufferedOutputStream out) throws IOException {
        boolean noreply = false;
        long delaySeconds = 0L;
        if (parts.length == 2) {
            if ("noreply".equalsIgnoreCase(parts[1])) {
                noreply = true;
            } else {
                delaySeconds = parseLong(parts[1], out);
                if (delaySeconds < 0) {
                    return true;
                }
            }
        } else if (parts.length == 3) {
            delaySeconds = parseLong(parts[1], out);
            if (delaySeconds < 0) {
                return true;
            }
            if (!"noreply".equalsIgnoreCase(parts[2])) {
                writeLine(out, "CLIENT_ERROR invalid arguments");
                out.flush();
                return true;
            }
            noreply = true;
        } else if (parts.length > 3) {
            writeLine(out, "CLIENT_ERROR invalid arguments");
            out.flush();
            return true;
        }

        cmdFlush.incrementAndGet();
        if (delaySeconds <= 0L) {
            clusterClient.clear();
            currItems.set(0L);
            flushDeadlineMillis.set(0L);
        } else {
            long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(delaySeconds);
            flushDeadlineMillis.set(deadline);
        }

        if (!noreply) {
            writeLine(out, "OK");
            out.flush();
        }
        return true;
    }

    private long parseLong(String value, BufferedOutputStream out) throws IOException {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0) {
                throw new NumberFormatException();
            }
            return parsed;
        } catch (NumberFormatException e) {
            writeLine(out, "CLIENT_ERROR numeric value expected");
            out.flush();
            return -1L;
        }
    }

    private void handleStats(BufferedOutputStream out) throws IOException {
        long now = System.currentTimeMillis();
        writeStat(out, "pid", ProcessHandle.current().pid());
        writeStat(out, "uptime", (now - startTime) / 1000);
        writeStat(out, "time", now / 1000);
        writeStat(out, "version", getVersion());
        writeStat(out, "curr_connections", currConnections.get());
        writeStat(out, "total_connections", totalConnections.get());
        writeStat(out, "cmd_get", cmdGet.get());
        writeStat(out, "cmd_set", cmdSet.get());
        writeStat(out, "cmd_touch", cmdTouch.get());
        writeStat(out, "cmd_flush", cmdFlush.get());
        writeStat(out, "get_hits", getHits.get());
        writeStat(out, "get_misses", getMisses.get());
        writeStat(out, "curr_items", currItems.get());
        writeStat(out, "total_items", totalItems.get());
        writeLine(out, "END");
        out.flush();
    }

    private void writeStat(BufferedOutputStream out, String name, Object value) throws IOException {
        writeLine(out, "STAT " + name + " " + value);
    }

    private void maybeApplyDelayedFlush()
    {
        long deadline = flushDeadlineMillis.get();
        if (deadline <= 0L) {
            return;
        }
        if (System.currentTimeMillis() >= deadline && flushDeadlineMillis.compareAndSet(deadline, 0L)) {
            clusterClient.clear();
            currItems.set(0L);
        }
    }

    private StoredValueCodec.StoredValue getEntry(String key)
    {
        String encoded = clusterClient.get(key);
        if (encoded == null) {
            return null;
        }
        StoredValueCodec.StoredValue entry = StoredValueCodec.decode(encoded);
        long now = System.currentTimeMillis();
        if (entry.expired(now)) {
            if (clusterClient.delete(key)) {
                decrementCurrItems();
            }
            return null;
        }
        return entry;
    }

    private boolean storeEntry(String key, StoredValueCodec.StoredValue entry, Duration ttl)
    {
        Duration effectiveTtl = ttl;
        if (effectiveTtl == null) {
            effectiveTtl = ttlFromExpireAt(entry.expireAt());
        }
        if (effectiveTtl != null && effectiveTtl.isZero()) {
            if (clusterClient.delete(key)) {
                decrementCurrItems();
            }
            return true;
        }
        return clusterClient.set(key, StoredValueCodec.encode(entry), effectiveTtl);
    }

    private long nextCas() {
        return casCounter.updateAndGet(prev -> prev == Long.MAX_VALUE ? 1L : prev + 1L);
    }

    private long computeExpireAt(Duration ttl)
    {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return 0L;
        }
        long now = System.currentTimeMillis();
        long millis = ttl.toMillis();
        long expireAt = now + millis;
        if (expireAt <= 0L) {
            return Long.MAX_VALUE;
        }
        return expireAt;
    }

    private Duration ttlFromExpireAt(long expireAt)
    {
        if (expireAt <= 0L || expireAt == Long.MAX_VALUE) {
            return null;
        }
        long remaining = expireAt - System.currentTimeMillis();
        if (remaining <= 0L) {
            return Duration.ZERO;
        }
        return Duration.ofMillis(remaining);
    }

    private enum CasUpdateStatus
    {
        SUCCESS,
        NOT_FOUND,
        CONFLICT,
        TOO_LARGE
    }

    private void incrementItems()
    {
        currItems.incrementAndGet();
        totalItems.incrementAndGet();
    }

    private void decrementCurrItems() {
        currItems.updateAndGet(prev -> Math.max(0L, prev - 1L));
    }

    private Duration parseExpiration(long exptime)
    {
        if (exptime < 0) {
            return Duration.ZERO;
        }
        if (exptime == 0) {
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
        return sb.isEmpty() ? null : sb.toString();
    }

    private void writeLine(BufferedOutputStream out, String line) throws IOException {
        out.write(line.getBytes(StandardCharsets.US_ASCII));
        out.write(CRLF);
    }

    private String getVersion() {
        Package pkg = CanCachedServer.class.getPackage();
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
        if (removalSubscription != null) {
            try {
                removalSubscription.close();
            } catch (Exception ignored) {
            }
        }
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

    private static final class NamedThreadFactory implements ThreadFactory
    {
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
