package com.can.net;

import com.can.cluster.ClusterClient;
import com.can.config.AppProperties;
import com.can.constants.CanCachedProtocol;
import com.can.core.CacheEngine;
import com.can.core.StoredValueCodec;
import com.can.net.protocol.CommandAction;
import com.can.net.protocol.CommandResult;
import com.can.net.protocol.ImmediateCommand;
import com.can.net.protocol.PendingStorageCommand;
import com.can.net.protocol.StorageCommand;
import io.quarkus.runtime.Startup;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
    private final Vertx vertx;
    private final ClusterClient clusterClient;
    private final AppProperties.Network networkConfig;
    private final int maxItemSize;
    private final int maxCasRetries;
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
    private NetServer netServer;
    private AutoCloseable removalSubscription;

    @Inject
    public CanCachedServer(Vertx vertx,
                           ClusterClient clusterClient,
                           AppProperties properties,
                           CacheEngine<String, String> localEngine)
    {
        this.vertx = Objects.requireNonNull(vertx, "vertx");
        this.clusterClient = Objects.requireNonNull(clusterClient, "clusterClient");
        this.networkConfig = Objects.requireNonNull(properties.network(), "networkConfig");
        var cancacheConfig = Objects.requireNonNull(properties.cancache(), "cancacheConfig");
        this.maxItemSize = Math.max(1, cancacheConfig.maxItemSizeBytes());
        this.maxCasRetries = Math.max(1, cancacheConfig.maxCasRetries());
        this.localEngine = Objects.requireNonNull(localEngine, "localEngine");
    }

    @PostConstruct
    void start()
    {
        NetServerOptions options = new NetServerOptions()
                .setHost(networkConfig.host())
                .setPort(networkConfig.port())
                .setTcpNoDelay(true)
                .setReuseAddress(true)
                .setAcceptBacklog(Math.max(1, networkConfig.backlog()));

        netServer = vertx.createNetServer(options);
        netServer.connectHandler(this::onClientConnected);
        try {
            netServer.listen().toCompletionStage().toCompletableFuture().join();
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to bind cancached port", e);
        }

        running = true;
        LOG.infof("cancached-compatible server listening on %s:%d", networkConfig.host(), netServer.actualPort());
        removalSubscription = localEngine.onRemoval(key -> decrementCurrItems());
    }

    private void onClientConnected(NetSocket socket)
    {
        if (!running) {
            socket.close();
            return;
        }
        currConnections.incrementAndGet();
        totalConnections.incrementAndGet();

        ConnectionContext context = new ConnectionContext(socket);
        socket.closeHandler(v -> {
            context.onClosed();
            currConnections.decrementAndGet();
        });
        socket.exceptionHandler(e -> {
            if (LOG.isDebugEnabled()) {
                LOG.debugf(e, "Client %s disconnected with error", socket.remoteAddress());
            }
            socket.close();
        });
        socket.handler(context::handleData);
    }

    private CommandAction parseCommand(String line)
    {
        String[] parts = line.trim().split("\\s+");
        if (parts.length == 0 || parts[0].isEmpty()) {
            return new ImmediateCommand(() -> {
                maybeApplyDelayedFlush();
                return CommandResult.continueWithoutResponse();
            });
        }
        String command = parts[0].toLowerCase(Locale.ROOT);
        return switch (command) {
            case CanCachedProtocol.SET,
                    CanCachedProtocol.ADD,
                    CanCachedProtocol.REPLACE,
                    CanCachedProtocol.APPEND,
                    CanCachedProtocol.PREPEND,
                    CanCachedProtocol.CAS -> prepareStorageCommand(command, parts);
            case CanCachedProtocol.GET -> new ImmediateCommand(() -> handleGet(parts, false));
            case CanCachedProtocol.GETS -> new ImmediateCommand(() -> handleGet(parts, true));
            case CanCachedProtocol.DELETE -> new ImmediateCommand(() -> handleDelete(parts));
            case CanCachedProtocol.INCR, CanCachedProtocol.DECR -> new ImmediateCommand(() -> handleIncrDecr(command, parts));
            case CanCachedProtocol.TOUCH -> new ImmediateCommand(() -> handleTouch(parts));
            case CanCachedProtocol.FLUSH_ALL -> new ImmediateCommand(() -> handleFlushAll(parts));
            case CanCachedProtocol.STATS -> new ImmediateCommand(this::handleStats);
            case CanCachedProtocol.VERSION -> new ImmediateCommand(this::handleVersion);
            case CanCachedProtocol.QUIT -> new ImmediateCommand(() -> {
                maybeApplyDelayedFlush();
                return CommandResult.terminate();
            });
            default -> new ImmediateCommand(() -> {
                maybeApplyDelayedFlush();
                return handleSimpleLine(CanCachedProtocol.ERROR);
            });
        };
    }

    private CommandAction prepareStorageCommand(String command, String[] parts)
    {
        boolean isCas = CanCachedProtocol.CAS.equals(command);
        int minParts = isCas ? 6 : 5;
        if (parts.length < minParts) {
            return new ImmediateCommand(() -> {
                maybeApplyDelayedFlush();
                return handleSimpleLine("CLIENT_ERROR bad command line format");
            });
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
            return new ImmediateCommand(() -> {
                maybeApplyDelayedFlush();
                return handleSimpleLine("CLIENT_ERROR numeric value expected");
            });
        }

        int noreplyIndex = isCas ? 6 : 5;
        boolean noreply = false;
        if (parts.length > noreplyIndex) {
            if (parts.length == noreplyIndex + 1 && "noreply".equalsIgnoreCase(parts[noreplyIndex])) {
                noreply = true;
            } else {
                return new ImmediateCommand(() -> {
                    maybeApplyDelayedFlush();
                    return handleSimpleLine("CLIENT_ERROR invalid arguments");
                });
            }
        }

        if (bytes < 0 || bytes > maxItemSize) {
            return new ImmediateCommand(() -> {
                maybeApplyDelayedFlush();
                return handleSimpleLine("CLIENT_ERROR bad data chunk");
            });
        }

        Duration ttl = parseExpiration(exptime);
        return new StorageCommand(new PendingStorageCommand(command, parts[1], flags, ttl, (int) bytes, noreply, isCas, casUnique));
    }

    private CommandResult handleStoragePayload(PendingStorageCommand pending, Buffer payload)
    {
        maybeApplyDelayedFlush();

        if (payload.length() < pending.totalLength()) {
            return handleSimpleLine("CLIENT_ERROR bad data chunk");
        }
        if (payload.getByte(pending.bytes()) != '\r' || payload.getByte(pending.bytes() + 1) != '\n') {
            return handleSimpleLine("CLIENT_ERROR bad data chunk");
        }

        byte[] valueBytes = payload.getBytes(0, pending.bytes());
        String key = pending.key();
        Duration ttl = pending.ttl();
        cmdSet.incrementAndGet();

        StoredValueCodec.StoredValue existing = getEntry(key);
        if (existing != null && existing.expired(System.currentTimeMillis())) {
            existing = null;
        }

        if (Duration.ZERO.equals(ttl)) {
            if (existing != null && clusterClient.delete(key)) {
                decrementCurrItems();
            }
            return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("STORED");
        }

        if (pending.isCas()) {
            return handleCasCommand(key, valueBytes, pending.flags(), ttl, pending.casUnique(), pending.noreply(), existing);
        }

        switch (pending.command()) {
            case CanCachedProtocol.ADD -> {
                if (existing != null) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_STORED");
                }
                StoredValueCodec.StoredValue entry = new StoredValueCodec.StoredValue(valueBytes, pending.flags(), nextCas(), computeExpireAt(ttl));
                if (!storeEntry(key, entry, ttl)) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_STORED");
                }
                incrementItems();
            }
            case CanCachedProtocol.REPLACE -> {
                if (existing == null) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_STORED");
                }
                StoredValueCodec.StoredValue entry = new StoredValueCodec.StoredValue(valueBytes, pending.flags(), nextCas(), computeExpireAt(ttl));
                if (!storeEntry(key, entry, ttl)) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_STORED");
                }
            }
            case CanCachedProtocol.APPEND -> {
                if (existing == null) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_STORED");
                }
                if ((long) existing.value().length + valueBytes.length > maxItemSize) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("SERVER_ERROR object too large");
                }
                CasUpdateStatus status = appendOrPrepend(key, existing, valueBytes, false);
                if (status == CasUpdateStatus.NOT_FOUND) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_STORED");
                }
                if (status == CasUpdateStatus.TOO_LARGE) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("SERVER_ERROR object too large");
                }
                if (status == CasUpdateStatus.CONFLICT) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("SERVER_ERROR cas conflict");
                }
            }
            case CanCachedProtocol.PREPEND -> {
                if (existing == null) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_STORED");
                }
                if ((long) existing.value().length + valueBytes.length > maxItemSize) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("SERVER_ERROR object too large");
                }
                CasUpdateStatus status = appendOrPrepend(key, existing, valueBytes, true);
                if (status == CasUpdateStatus.NOT_FOUND) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_STORED");
                }
                if (status == CasUpdateStatus.TOO_LARGE) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("SERVER_ERROR object too large");
                }
                if (status == CasUpdateStatus.CONFLICT) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("SERVER_ERROR cas conflict");
                }
            }
            case CanCachedProtocol.SET -> {
                StoredValueCodec.StoredValue entry = new StoredValueCodec.StoredValue(valueBytes, pending.flags(), nextCas(), computeExpireAt(ttl));
                if (!storeEntry(key, entry, ttl)) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_STORED");
                }
                if (existing == null) {
                    incrementItems();
                }
            }
            default -> {
                StoredValueCodec.StoredValue entry = new StoredValueCodec.StoredValue(valueBytes, pending.flags(), nextCas(), computeExpireAt(ttl));
                if (!storeEntry(key, entry, ttl)) {
                    return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_STORED");
                }
                if (existing == null) {
                    incrementItems();
                }
            }
        }

        return pending.noreply() ? CommandResult.continueWithoutResponse() : handleSimpleLine("STORED");
    }

    private CommandResult handleCasCommand(String key,
                                           byte[] value,
                                           int flags,
                                           Duration ttl,
                                           long casUnique,
                                           boolean noreply,
                                           StoredValueCodec.StoredValue existing)
    {
        if (existing == null) {
            return noreply ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_FOUND");
        }
        if (existing.cas() != casUnique) {
            return noreply ? CommandResult.continueWithoutResponse() : handleSimpleLine("EXISTS");
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
            if (noreply) {
                return CommandResult.continueWithoutResponse();
            }
            if (latest == null) {
                return handleSimpleLine("NOT_FOUND");
            }
            return handleSimpleLine("EXISTS");
        }
        return noreply ? CommandResult.continueWithoutResponse() : handleSimpleLine("STORED");
    }

    private CasUpdateStatus appendOrPrepend(String key,
                                            StoredValueCodec.StoredValue snapshot,
                                            byte[] addition,
                                            boolean prepend)
    {
        StoredValueCodec.StoredValue current = snapshot;
        for (int attempt = 0; attempt < maxCasRetries; attempt++) {
            if (current == null) {
                return CasUpdateStatus.NOT_FOUND;
            }
            if ((long) current.value().length + addition.length > maxItemSize) {
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

    private CommandResult handleGet(String[] parts, boolean includeCas)
    {
        maybeApplyDelayedFlush();
        if (parts.length < 2) {
            return handleSimpleLine("CLIENT_ERROR bad command line format");
        }
        cmdGet.incrementAndGet();
        long now = System.currentTimeMillis();
        Buffer response = Buffer.buffer();
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
            writeLine(response, header);
            response.appendBytes(entry.value());
            response.appendBytes(CRLF);
        }
        writeLine(response, "END");
        return CommandResult.continueWith(response);
    }

    private CommandResult handleDelete(String[] parts)
    {
        maybeApplyDelayedFlush();
        if (parts.length < 2) {
            return handleSimpleLine("CLIENT_ERROR bad command line format");
        }
        boolean noreply = parts.length == 3 && "noreply".equalsIgnoreCase(parts[2]);
        if (parts.length > 3 || (parts.length == 3 && !noreply)) {
            return handleSimpleLine("CLIENT_ERROR invalid arguments");
        }
        boolean removed = clusterClient.delete(parts[1]);
        if (removed) {
            decrementCurrItems();
        }
        if (noreply) {
            return CommandResult.continueWithoutResponse();
        }
        return handleSimpleLine(removed ? "DELETED" : "NOT_FOUND");
    }

    private CommandResult handleIncrDecr(String command, String[] parts)
    {
        maybeApplyDelayedFlush();
        if (parts.length < 3) {
            return handleSimpleLine("CLIENT_ERROR bad command line format");
        }
        boolean noreply = parts.length == 4 && "noreply".equalsIgnoreCase(parts[3]);
        if (parts.length > 4 || (parts.length == 4 && !noreply)) {
            return handleSimpleLine("CLIENT_ERROR invalid arguments");
        }
        BigInteger delta;
        try {
            delta = new BigInteger(parts[2]);
            if (delta.signum() < 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            return handleSimpleLine("CLIENT_ERROR invalid numeric delta");
        }

        String key = parts[1];
        StoredValueCodec.StoredValue current = getEntry(key);
        for (int attempt = 0; attempt < maxCasRetries; attempt++) {
            if (current == null) {
                return noreply ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_FOUND");
            }
            String currentValue = new String(current.value(), StandardCharsets.US_ASCII);
            if (!currentValue.chars().allMatch(Character::isDigit)) {
                return handleSimpleLine("CLIENT_ERROR cannot increment or decrement non-numeric value");
            }
            BigInteger numeric = new BigInteger(currentValue);
            BigInteger updated = CanCachedProtocol.INCR.equals(command) ? numeric.add(delta) : numeric.subtract(delta);
            if (updated.signum() < 0) {
                updated = BigInteger.ZERO;
            }
            byte[] newValue = updated.toString().getBytes(StandardCharsets.US_ASCII);
            if (newValue.length > maxItemSize) {
                return handleSimpleLine("SERVER_ERROR object too large");
            }
            StoredValueCodec.StoredValue candidate = new StoredValueCodec.StoredValue(newValue, current.flags(), nextCas(), current.expireAt());
            Duration ttl = ttlFromExpireAt(current.expireAt());
            if (clusterClient.compareAndSwap(key, StoredValueCodec.encode(candidate), current.cas(), ttl)) {
                return noreply ? CommandResult.continueWithoutResponse() : CommandResult.continueWith(lineBuffer(updated.toString()));
            }
            current = getEntry(key);
        }
        return noreply ? CommandResult.continueWithoutResponse() : handleSimpleLine("SERVER_ERROR cas conflict");
    }

    private CommandResult handleTouch(String[] parts)
    {
        maybeApplyDelayedFlush();
        if (parts.length < 3) {
            return handleSimpleLine("CLIENT_ERROR bad command line format");
        }
        boolean noreply = parts.length == 4 && "noreply".equalsIgnoreCase(parts[3]);
        if (parts.length > 4 || (parts.length == 4 && !noreply)) {
            return handleSimpleLine("CLIENT_ERROR invalid arguments");
        }
        long exptime;
        try {
            exptime = Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            return handleSimpleLine("CLIENT_ERROR numeric value expected");
        }
        Duration ttl = parseExpiration(exptime);
        if (Duration.ZERO.equals(ttl)) {
            if (clusterClient.delete(parts[1])) {
                decrementCurrItems();
            }
            return noreply ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_FOUND");
        }
        StoredValueCodec.StoredValue current = getEntry(parts[1]);
        for (int attempt = 0; attempt < maxCasRetries; attempt++) {
            if (current == null) {
                return noreply ? CommandResult.continueWithoutResponse() : handleSimpleLine("NOT_FOUND");
            }
            long expireAt = computeExpireAt(ttl);
            StoredValueCodec.StoredValue candidate = new StoredValueCodec.StoredValue(current.value(), current.flags(), current.cas(), expireAt);
            Duration effectiveTtl = ttl;
            if (effectiveTtl == null) {
                effectiveTtl = ttlFromExpireAt(expireAt);
            }
            if (clusterClient.compareAndSwap(parts[1], StoredValueCodec.encode(candidate), current.cas(), effectiveTtl)) {
                cmdTouch.incrementAndGet();
                return noreply ? CommandResult.continueWithoutResponse() : handleSimpleLine("TOUCHED");
            }
            current = getEntry(parts[1]);
        }
        return noreply ? CommandResult.continueWithoutResponse() : handleSimpleLine("SERVER_ERROR cas conflict");
    }

    private CommandResult handleFlushAll(String[] parts)
    {
        maybeApplyDelayedFlush();
        boolean noreply = false;
        long delaySeconds = 0L;
        if (parts.length == 2) {
            if ("noreply".equalsIgnoreCase(parts[1])) {
                noreply = true;
            } else {
                Long parsed = parseNonNegativeLong(parts[1]);
                if (parsed == null) {
                    return handleSimpleLine("CLIENT_ERROR numeric value expected");
                }
                delaySeconds = parsed;
            }
        } else if (parts.length == 3) {
            Long parsed = parseNonNegativeLong(parts[1]);
            if (parsed == null) {
                return handleSimpleLine("CLIENT_ERROR numeric value expected");
            }
            delaySeconds = parsed;
            if (!"noreply".equalsIgnoreCase(parts[2])) {
                return handleSimpleLine("CLIENT_ERROR invalid arguments");
            }
            noreply = true;
        } else if (parts.length > 3) {
            return handleSimpleLine("CLIENT_ERROR invalid arguments");
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

        return noreply ? CommandResult.continueWithoutResponse() : handleSimpleLine("OK");
    }

    private Long parseNonNegativeLong(String value)
    {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0) {
                throw new NumberFormatException();
            }
            return parsed;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private CommandResult handleStats()
    {
        maybeApplyDelayedFlush();
        long now = System.currentTimeMillis();
        Buffer out = Buffer.buffer();
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
        return CommandResult.continueWith(out);
    }

    private CommandResult handleVersion()
    {
        maybeApplyDelayedFlush();
        return handleSimpleLine("VERSION " + getVersion());
    }

    private CommandResult handleSimpleLine(String line)
    {
        return CommandResult.continueWith(lineBuffer(line));
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

    private long nextCas()
    {
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

    private void decrementCurrItems()
    {
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

    private void writeLine(Buffer buffer, String line)
    {
        buffer.appendString(line, StandardCharsets.US_ASCII.name());
        buffer.appendBytes(CRLF);
    }

    private Buffer lineBuffer(String line)
    {
        Buffer buffer = Buffer.buffer(line.length() + CRLF.length);
        writeLine(buffer, line);
        return buffer;
    }

    private void writeStat(Buffer out, String name, Object value)
    {
        writeLine(out, "STAT " + name + " " + value);
    }

    private String getVersion()
    {
        Package pkg = CanCachedServer.class.getPackage();
        if (pkg != null && pkg.getImplementationVersion() != null) {
            return pkg.getImplementationVersion();
        }
        return "0.0.1";
    }

    public int port()
    {
        return netServer != null ? netServer.actualPort() : networkConfig.port();
    }

    @PreDestroy
    @Override
    public void close()
    {
        running = false;
        if (removalSubscription != null) {
            try {
                removalSubscription.close();
            } catch (Exception ignored) {
            }
        }
        if (netServer != null) {
            try {
                netServer.close().toCompletionStage().toCompletableFuture().join();
            } catch (RuntimeException e) {
                LOG.debug("Failed to close net server", e);
            }
        }
    }

    private final class ConnectionContext
    {
        private final NetSocket socket;
        private Buffer buffer = Buffer.buffer();
        private PendingStorageCommand pendingStorage;
        private boolean closed;
        private boolean processing;

        private ConnectionContext(NetSocket socket)
        {
            this.socket = socket;
        }

        private void handleData(Buffer data)
        {
            if (closed) return;
            buffer.appendBuffer(data);
            processBuffer();
        }

        private void onClosed()
        {
            closed = true;
            pendingStorage = null;
            buffer = Buffer.buffer();
        }

        private void processBuffer()
        {
            while (!closed && !processing)
            {
                if (pendingStorage != null)
                {
                    if (buffer.length() < pendingStorage.totalLength()) return;
                    Buffer payload = buffer.getBuffer(0, pendingStorage.totalLength());
                    buffer = buffer.getBuffer(pendingStorage.totalLength(), buffer.length());
                    PendingStorageCommand command = pendingStorage;
                    pendingStorage = null;
                    executeCommand(() -> handleStoragePayload(command, payload));
                    return;
                }

                int lineEnd = indexOfCrlf(buffer);
                if (lineEnd < 0) {
                    return;
                }
                String line = buffer.getString(0, lineEnd);
                buffer = buffer.getBuffer(lineEnd + CRLF.length, buffer.length());
                if (line.isEmpty()) {
                    continue;
                }

                CommandAction action = parseCommand(line);
                if (action instanceof ImmediateCommand immediate) {
                    executeCommand(immediate.executor());
                    return;
                }
                if (action instanceof StorageCommand storage) {
                    pendingStorage = storage.pending();
                    if (buffer.length() >= pendingStorage.totalLength()) {
                        continue;
                    }
                }
                return;
            }
        }

        private void executeCommand(Supplier<CommandResult> executor)
        {
            processing = true;
            vertx.<CommandResult>executeBlocking(promise -> {
                try {
                    promise.complete(executor.get());
                } catch (Throwable t) {
                    promise.fail(t);
                }
            }, false, ar -> {
                processing = false;
                if (closed) {
                    return;
                }
                if (ar.succeeded()) {
                    CommandResult result = ar.result();
                    if (result != null && result.response() != null && result.response().length() > 0) {
                        socket.write(result.response());
                    }
                    if (result != null && !result.keepAlive()) {
                        close();
                        return;
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debugf(ar.cause(), "Client %s disconnected with error", socket.remoteAddress());
                    }
                    close();
                    return;
                }
                processBuffer();
            });
        }

        private int indexOfCrlf(Buffer buffer)
        {
            int length = buffer.length();
            for (int i = 0; i < length - 1; i++) {
                if (buffer.getByte(i) == '\r' && buffer.getByte(i + 1) == '\n') {
                    return i;
                }
            }
            return -1;
        }

        private void close()
        {
            if (!closed) {
                closed = true;
                socket.close();
            }
        }
    }
}
