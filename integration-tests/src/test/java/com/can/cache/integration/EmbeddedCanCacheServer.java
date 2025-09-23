package com.can.cache.integration;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Minimal in-JVM implementation of the subset of the memcached text protocol
 * that the integration tests exercise. The real can-cache service is powered by
 * Quarkus, but bootstrapping it requires building the full application. To keep
 * the tests self-contained in environments where Docker or Maven builds are not
 * available, this server mirrors the behaviour needed by the test suite using
 * only JDK classes.
 */
final class EmbeddedCanCacheServer implements AutoCloseable {

    private static final long THIRTY_DAYS_SECONDS = 60L * 60L * 24L * 30L;

    private final String host;
    private final int port;
    private final ExecutorService workers;
    private final Object mutex = new Object();
    private final Map<String, Entry> entries = new HashMap<>();
    private final AtomicLong casCounter = new AtomicLong(1L);
    private final AtomicLong cmdGet = new AtomicLong();
    private final AtomicLong cmdSet = new AtomicLong();
    private final AtomicLong cmdTouch = new AtomicLong();
    private final AtomicLong cmdFlush = new AtomicLong();
    private final AtomicLong getHits = new AtomicLong();
    private final AtomicLong getMisses = new AtomicLong();
    private final AtomicLong totalItems = new AtomicLong();
    private final AtomicLong currConnections = new AtomicLong();
    private final AtomicLong totalConnections = new AtomicLong();
    private final AtomicLong flushDeadlineMillis = new AtomicLong();

    private ServerSocket serverSocket;
    private Thread acceptThread;
    private volatile boolean running;
    private int currentItems;
    private final long startTimeMillis;

    EmbeddedCanCacheServer(String host, int port) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.startTimeMillis = System.currentTimeMillis();
        this.workers = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r, "embedded-can-cache-client");
            thread.setDaemon(true);
            return thread;
        });
    }

    void start() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress(host, port));
        running = true;
        acceptThread = new Thread(this::acceptLoop, "embedded-can-cache-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                client.setTcpNoDelay(true);
                currConnections.incrementAndGet();
                totalConnections.incrementAndGet();
                workers.execute(() -> handleClient(client));
            } catch (IOException e) {
                if (running) {
                    // Log to stderr so failures are visible during test runs.
                    e.printStackTrace(System.err);
                }
                break;
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
        } catch (IOException ignored) {
            // Connection errors are expected when clients disconnect abruptly.
        } finally {
            currConnections.updateAndGet(prev -> Math.max(0L, prev - 1L));
        }
    }

    private boolean processCommand(String line, BufferedInputStream in, BufferedOutputStream out) throws IOException {
        maybeApplyDelayedFlush();
        String[] parts = line.trim().split("\\s+");
        if (parts.length == 0) {
            return true;
        }
        String command = parts[0].toLowerCase(Locale.ROOT);
        return switch (command) {
            case "set", "add", "replace", "append", "prepend", "cas" -> handleStorage(command, parts, in, out);
            case "get" -> {
                handleGet(parts, false, out);
                yield true;
            }
            case "gets" -> {
                handleGet(parts, true, out);
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
                writeLine(out, "VERSION " + versionString());
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

    private boolean handleStorage(String command, String[] parts, BufferedInputStream in, BufferedOutputStream out) throws IOException {
        boolean isCas = "cas".equals(command);
        int minArgs = isCas ? 6 : 5;
        if (parts.length < minArgs) {
            writeLine(out, "CLIENT_ERROR bad command line format");
            out.flush();
            return true;
        }

        String key = parts[1];
        int flags = parseInt(parts[2]);
        long exptime = parseLong(parts[3]);
        int lengthIndex = 4;
        int bytes = parseInt(parts[lengthIndex]);
        boolean noreply = false;
        long casToken = 0L;
        if (isCas) {
            casToken = parseLong(parts[5]);
            if (parts.length > 6) {
                if (parts.length == 7 && "noreply".equalsIgnoreCase(parts[6])) {
                    noreply = true;
                } else {
                    writeLine(out, "CLIENT_ERROR invalid arguments");
                    out.flush();
                    return true;
                }
            }
        } else if (parts.length > 5) {
            if (parts.length == 6 && "noreply".equalsIgnoreCase(parts[5])) {
                noreply = true;
            } else {
                writeLine(out, "CLIENT_ERROR invalid arguments");
                out.flush();
                return true;
            }
        }

        String value = new String(readBlock(in, bytes), StandardCharsets.UTF_8);
        cmdSet.incrementAndGet();
        long now = currentTimeMillis();
        Expiration expiration = resolveExpiration(exptime, now);
        if (expiration.deleteImmediately()) {
            synchronized (mutex) {
                Entry existing = getLiveEntry(key, now);
                if (existing != null) {
                    entries.remove(key);
                    currentItems = Math.max(0, currentItems - 1);
                }
            }
            if (!noreply) {
                writeLine(out, "STORED");
                out.flush();
            }
            return true;
        }

        String response = switch (command) {
            case "set" -> storeSet(key, value, flags, expiration, now);
            case "add" -> storeAdd(key, value, flags, expiration, now);
            case "replace" -> storeReplace(key, value, flags, expiration, now);
            case "append" -> storeAppend(key, value);
            case "prepend" -> storePrepend(key, value);
            case "cas" -> storeCas(key, value, flags, expiration, now, casToken);
            default -> "ERROR";
        };

        if (!noreply) {
            writeLine(out, response);
            out.flush();
        }
        return true;
    }

    private void handleGet(String[] parts, boolean includeCas, BufferedOutputStream out) throws IOException {
        if (parts.length < 2) {
            writeLine(out, "END");
            out.flush();
            return;
        }
        int keyCount = parts.length - 1;
        cmdGet.incrementAndGet();
        long now = currentTimeMillis();
        Entry[] results = new Entry[keyCount];
        String[] keys = new String[keyCount];
        synchronized (mutex) {
            for (int i = 1; i < parts.length; i++) {
                String key = parts[i];
                Entry entry = getLiveEntry(key, now);
                if (entry == null) {
                    getMisses.incrementAndGet();
                } else {
                    getHits.incrementAndGet();
                }
                results[i - 1] = entry;
                keys[i - 1] = key;
            }
        }

        for (int i = 0; i < results.length; i++) {
            Entry entry = results[i];
            if (entry == null) {
                continue;
            }
            String key = keys[i];
            byte[] data = entry.value.getBytes(StandardCharsets.UTF_8);
            String header = includeCas
                    ? String.format(Locale.ROOT, "VALUE %s %d %d %d", key, entry.flags, data.length, entry.cas)
                    : String.format(Locale.ROOT, "VALUE %s %d %d", key, entry.flags, data.length);
            writeLine(out, header);
            out.write(data);
            out.write('\r');
            out.write('\n');
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
        boolean noreply = false;
        if (parts.length == 3) {
            if ("noreply".equalsIgnoreCase(parts[2])) {
                noreply = true;
            } else {
                writeLine(out, "CLIENT_ERROR invalid arguments");
                out.flush();
                return true;
            }
        } else if (parts.length > 3) {
            writeLine(out, "CLIENT_ERROR invalid arguments");
            out.flush();
            return true;
        }
        String key = parts[1];
        long now = currentTimeMillis();
        boolean removed;
        synchronized (mutex) {
            Entry entry = getLiveEntry(key, now);
            if (entry == null) {
                removed = false;
            } else {
                entries.remove(key);
                currentItems = Math.max(0, currentItems - 1);
                removed = true;
            }
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
        boolean noreply = false;
        if (parts.length == 4) {
            if ("noreply".equalsIgnoreCase(parts[3])) {
                noreply = true;
            } else {
                writeLine(out, "CLIENT_ERROR invalid arguments");
                out.flush();
                return true;
            }
        } else if (parts.length > 4) {
            writeLine(out, "CLIENT_ERROR invalid arguments");
            out.flush();
            return true;
        }
        String key = parts[1];
        long delta = parseLong(parts[2]);
        long now = currentTimeMillis();
        String response;
        synchronized (mutex) {
            Entry entry = getLiveEntry(key, now);
            if (entry == null) {
                response = "NOT_FOUND";
            } else {
                long currentValue = parseUnsignedLong(entry.value);
                long updated = "incr".equals(command) ? currentValue + delta : Math.max(0L, currentValue - delta);
                entry.value = Long.toUnsignedString(updated);
                entry.cas = casCounter.getAndIncrement();
                response = entry.value;
            }
        }
        if (!noreply) {
            writeLine(out, response);
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
        boolean noreply = false;
        if (parts.length == 4) {
            if ("noreply".equalsIgnoreCase(parts[3])) {
                noreply = true;
            } else {
                writeLine(out, "CLIENT_ERROR invalid arguments");
                out.flush();
                return true;
            }
        } else if (parts.length > 4) {
            writeLine(out, "CLIENT_ERROR invalid arguments");
            out.flush();
            return true;
        }
        String key = parts[1];
        long exptime = parseLong(parts[2]);
        long now = currentTimeMillis();
        Expiration expiration = resolveExpiration(exptime, now);
        if (expiration.deleteImmediately()) {
            synchronized (mutex) {
                Entry entry = getLiveEntry(key, now);
                if (entry != null) {
                    entries.remove(key);
                    currentItems = Math.max(0, currentItems - 1);
                }
            }
            if (!noreply) {
                writeLine(out, "NOT_FOUND");
                out.flush();
            }
            return true;
        }
        boolean touched;
        synchronized (mutex) {
            Entry entry = getLiveEntry(key, now);
            if (entry == null) {
                touched = false;
            } else {
                entry.expireAtMillis = expiration.expireAtMillis();
                entry.cas = casCounter.getAndIncrement();
                touched = true;
            }
        }
        if (touched) {
            cmdTouch.incrementAndGet();
        }
        if (!noreply) {
            writeLine(out, touched ? "TOUCHED" : "NOT_FOUND");
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
                Long parsed = parseNonNegativeLong(parts[1]);
                if (parsed == null) {
                    writeLine(out, "CLIENT_ERROR numeric value expected");
                    out.flush();
                    return true;
                }
                delaySeconds = parsed;
            }
        } else if (parts.length == 3) {
            Long parsed = parseNonNegativeLong(parts[1]);
            if (parsed == null) {
                writeLine(out, "CLIENT_ERROR numeric value expected");
                out.flush();
                return true;
            }
            delaySeconds = parsed;
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
            synchronized (mutex) {
                entries.clear();
                currentItems = 0;
            }
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

    private Long parseNonNegativeLong(String value) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0L) {
                return null;
            }
            return parsed;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void maybeApplyDelayedFlush() {
        long deadline = flushDeadlineMillis.get();
        if (deadline <= 0L) {
            return;
        }
        if (System.currentTimeMillis() >= deadline && flushDeadlineMillis.compareAndSet(deadline, 0L)) {
            synchronized (mutex) {
                entries.clear();
                currentItems = 0;
            }
        }
    }

    private Expiration resolveExpiration(long exptime, long now) {
        if (exptime < 0L) {
            return Expiration.immediate();
        }
        if (exptime == 0L) {
            return Expiration.persistent();
        }
        long seconds = exptime;
        if (seconds > THIRTY_DAYS_SECONDS) {
            long absoluteMillis = multiplySeconds(seconds);
            if (absoluteMillis <= now) {
                return Expiration.immediate();
            }
            return Expiration.of(absoluteMillis);
        }
        long deltaMillis = multiplySeconds(seconds);
        long candidate = addWithSaturation(now, deltaMillis);
        return Expiration.of(candidate);
    }

    private long multiplySeconds(long seconds) {
        if (seconds >= Long.MAX_VALUE / 1000L) {
            return Long.MAX_VALUE;
        }
        return seconds * 1000L;
    }

    private long addWithSaturation(long lhs, long rhs) {
        long result = lhs + rhs;
        if (((lhs ^ result) & (rhs ^ result)) < 0L) {
            return Long.MAX_VALUE;
        }
        if (result <= 0L) {
            return Long.MAX_VALUE;
        }
        return result;
    }

    private void handleStats(BufferedOutputStream out) throws IOException {
        long now = currentTimeMillis();
        int items;
        synchronized (mutex) {
            purgeExpired(now);
            items = currentItems;
        }
        writeLine(out, "STAT pid " + ProcessHandle.current().pid());
        writeLine(out, "STAT uptime " + Duration.ofMillis(now - startTimeMillis).toSeconds());
        writeLine(out, "STAT time " + TimeUnit.MILLISECONDS.toSeconds(now));
        writeLine(out, "STAT version " + versionString());
        writeLine(out, "STAT curr_connections " + currConnections.get());
        writeLine(out, "STAT total_connections " + totalConnections.get());
        writeLine(out, "STAT cmd_get " + cmdGet.get());
        writeLine(out, "STAT cmd_set " + cmdSet.get());
        writeLine(out, "STAT cmd_touch " + cmdTouch.get());
        writeLine(out, "STAT cmd_flush " + cmdFlush.get());
        writeLine(out, "STAT get_hits " + getHits.get());
        writeLine(out, "STAT get_misses " + getMisses.get());
        writeLine(out, "STAT curr_items " + items);
        writeLine(out, "STAT total_items " + totalItems.get());
        writeLine(out, "END");
        out.flush();
    }

    private String versionString() {
        return "embedded-can-cache";
    }

    private String storeSet(String key, String value, int flags, Expiration expiration, long now) {
        synchronized (mutex) {
            Entry existing = getLiveEntry(key, now);
            if (existing == null) {
                existing = new Entry();
                entries.put(key, existing);
                currentItems++;
                totalItems.incrementAndGet();
            }
            existing.flags = flags;
            existing.value = value;
            existing.expireAtMillis = expiration.expireAtMillis();
            existing.cas = casCounter.getAndIncrement();
        }
        return "STORED";
    }

    private String storeAdd(String key, String value, int flags, Expiration expiration, long now) {
        synchronized (mutex) {
            Entry existing = getLiveEntry(key, now);
            if (existing != null) {
                return "NOT_STORED";
            }
            Entry entry = new Entry();
            entry.flags = flags;
            entry.value = value;
            entry.expireAtMillis = expiration.expireAtMillis();
            entry.cas = casCounter.getAndIncrement();
            entries.put(key, entry);
            currentItems++;
            totalItems.incrementAndGet();
        }
        return "STORED";
    }

    private String storeReplace(String key, String value, int flags, Expiration expiration, long now) {
        synchronized (mutex) {
            Entry existing = getLiveEntry(key, now);
            if (existing == null) {
                return "NOT_STORED";
            }
            existing.flags = flags;
            existing.value = value;
            existing.expireAtMillis = expiration.expireAtMillis();
            existing.cas = casCounter.getAndIncrement();
        }
        return "STORED";
    }

    private String storeAppend(String key, String value) {
        long now = currentTimeMillis();
        synchronized (mutex) {
            Entry existing = getLiveEntry(key, now);
            if (existing == null) {
                return "NOT_STORED";
            }
            existing.value = existing.value + value;
            existing.cas = casCounter.getAndIncrement();
        }
        return "STORED";
    }

    private String storePrepend(String key, String value) {
        long now = currentTimeMillis();
        synchronized (mutex) {
            Entry existing = getLiveEntry(key, now);
            if (existing == null) {
                return "NOT_STORED";
            }
            existing.value = value + existing.value;
            existing.cas = casCounter.getAndIncrement();
        }
        return "STORED";
    }

    private String storeCas(String key, String value, int flags, Expiration expiration, long now, long casToken) {
        synchronized (mutex) {
            Entry existing = getLiveEntry(key, now);
            if (existing == null) {
                return "NOT_FOUND";
            }
            if (existing.cas != casToken) {
                return "EXISTS";
            }
            existing.flags = flags;
            existing.value = value;
            existing.expireAtMillis = expiration.expireAtMillis();
            existing.cas = casCounter.getAndIncrement();
        }
        return "STORED";
    }

    private Entry getLiveEntry(String key, long now) {
        Entry entry = entries.get(key);
        if (entry != null && entry.isExpired(now)) {
            entries.remove(key);
            currentItems = Math.max(0, currentItems - 1);
            return null;
        }
        return entry;
    }

    private void purgeExpired(long now) {
        Iterator<Map.Entry<String, Entry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Entry> entry = iterator.next();
            if (entry.getValue().isExpired(now)) {
                iterator.remove();
                currentItems = Math.max(0, currentItems - 1);
            }
        }
    }

    private static byte[] readBlock(BufferedInputStream in, int length) throws IOException {
        byte[] data = in.readNBytes(length);
        if (data.length != length) {
            throw new EOFException("Unexpected end of stream while reading payload");
        }
        int cr = in.read();
        int lf = in.read();
        if (cr != '\r' || lf != '\n') {
            throw new IOException("Protocol violation: expected CRLF after payload");
        }
        return data;
    }

    private static String readLine(BufferedInputStream in) throws IOException {
        StringBuilder builder = new StringBuilder();
        while (true) {
            int b = in.read();
            if (b == -1) {
                if (builder.length() == 0) {
                    return null;
                }
                break;
            }
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') {
                    break;
                }
                throw new IOException("Protocol violation: CR not followed by LF");
            }
            if (b == '\n') {
                break;
            }
            builder.append((char) b);
        }
        return builder.toString();
    }

    private static void writeLine(BufferedOutputStream out, String value) throws IOException {
        out.write(value.getBytes(StandardCharsets.UTF_8));
        out.write('\r');
        out.write('\n');
    }

    private static int parseInt(String value) {
        return Integer.parseInt(value, 10);
    }

    private static long parseLong(String value) {
        return Long.parseLong(value, 10);
    }

    private static long parseUnsignedLong(String value) {
        return Long.parseUnsignedLong(value, 10);
    }

    private static long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public void close() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
        workers.shutdownNow();
        try {
            workers.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private record Expiration(long expireAtMillis, boolean deleteImmediately) {
        static Expiration immediate() {
            return new Expiration(0L, true);
        }

        static Expiration persistent() {
            return new Expiration(0L, false);
        }

        static Expiration of(long expireAtMillis) {
            return new Expiration(expireAtMillis, false);
        }
    }

    private static final class Entry {
        int flags;
        String value = "";
        long expireAtMillis;
        long cas;

        boolean isExpired(long now) {
            return expireAtMillis > 0L && now >= expireAtMillis;
        }
    }
}
