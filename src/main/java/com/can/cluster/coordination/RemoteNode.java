package com.can.cluster.coordination;

import com.can.cluster.Node;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Bir başka can-cache örneğinin replikasyon sunucusuna TCP üzerinden bağlanarak
 * {@link Node} sözleşmesini uygulayan hafif bir vekil düğümdür. Yazma işlemleri
 * hedef düğümün bellek motoruna doğrudan iletilir, okuma ve silme çağrıları ise
 * aynı protokol üzerinden yanıtlanır. Vert.x {@link NetClient} altyapısı
 * sayesinde bağlantılar havuzlanarak yeniden kullanılır ve tüm ağ işlemleri
 * asenkron şekilde yürütülür.
 */
public final class RemoteNode implements Node<String, String>, AutoCloseable
{
    private static final byte CMD_SET = 'S';
    private static final byte CMD_CAS = 'X';
    private static final byte CMD_GET = 'G';
    private static final byte CMD_DELETE = 'D';
    private static final byte CMD_CLEAR = 'C';
    private static final byte RESP_OK = 'O';
    private static final byte RESP_HIT = 'H';
    private static final byte RESP_MISS = 'M';
    private static final byte RESP_TRUE = 'T';
    private static final byte RESP_FALSE = 'F';

    private final String id;
    private final String host;
    private final int port;
    private final int connectTimeoutMillis;
    private final long requestTimeoutMillis;
    private final Vertx vertx;
    private final NetClient netClient;
    private final int maxPoolSize;
    private final BlockingQueue<PooledConnection> pool;
    private final Set<PooledConnection> allConnections = ConcurrentHashMap.newKeySet();
    private final AtomicInteger openConnections = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();

    public RemoteNode(String id, String host, int port, int connectTimeoutMillis, Vertx vertx)
    {
        this.id = Objects.requireNonNull(id, "id");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.connectTimeoutMillis = Math.max(100, connectTimeoutMillis);
        this.requestTimeoutMillis = Math.max(5000L, this.connectTimeoutMillis * 2L);
        this.vertx = Objects.requireNonNull(vertx, "vertx");
        this.maxPoolSize = Math.max(2, Runtime.getRuntime().availableProcessors());
        this.pool = new LinkedBlockingQueue<>(maxPoolSize);

        NetClientOptions options = new NetClientOptions()
                .setConnectTimeout(this.connectTimeoutMillis)
                .setTcpNoDelay(true)
                .setReuseAddress(true);
        this.netClient = vertx.createNetClient(options);
    }

    @Override
    public boolean set(String key, String value, Duration ttl)
    {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        long expireAt = expiryMillis(ttl);
        Buffer request = Buffer.buffer(1 + 4 + 4 + 8 + keyBytes.length + valueBytes.length)
                .appendByte(CMD_SET)
                .appendInt(keyBytes.length)
                .appendInt(valueBytes.length)
                .appendLong(expireAt)
                .appendBytes(keyBytes)
                .appendBytes(valueBytes);
        return execute(connection -> send(connection, request, new BooleanResponseParser(RESP_TRUE, RESP_FALSE)));
    }

    @Override
    public String get(String key)
    {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        Buffer request = Buffer.buffer(1 + 4 + keyBytes.length)
                .appendByte(CMD_GET)
                .appendInt(keyBytes.length)
                .appendBytes(keyBytes);
        return execute(connection -> send(connection, request, new GetResponseParser()));
    }

    @Override
    public boolean delete(String key)
    {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        Buffer request = Buffer.buffer(1 + 4 + keyBytes.length)
                .appendByte(CMD_DELETE)
                .appendInt(keyBytes.length)
                .appendBytes(keyBytes);
        return execute(connection -> send(connection, request, new BooleanResponseParser(RESP_TRUE, RESP_FALSE)));
    }

    @Override
    public boolean compareAndSwap(String key, String value, long expectedCas, Duration ttl)
    {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        long expireAt = expiryMillis(ttl);
        Buffer request = Buffer.buffer(1 + 4 + 4 + 8 + 8 + keyBytes.length + valueBytes.length)
                .appendByte(CMD_CAS)
                .appendInt(keyBytes.length)
                .appendInt(valueBytes.length)
                .appendLong(expireAt)
                .appendLong(expectedCas)
                .appendBytes(keyBytes)
                .appendBytes(valueBytes);
        return execute(connection -> send(connection, request, new BooleanResponseParser(RESP_TRUE, RESP_FALSE)));
    }

    @Override
    public void clear()
    {
        Buffer request = Buffer.buffer(1).appendByte(CMD_CLEAR);
        execute(connection -> send(connection, request, new ClearResponseParser()));
    }

    @Override
    public String id()
    {
        return id;
    }

    private <T> T execute(Function<PooledConnection, CompletableFuture<T>> action)
    {
        if (closed.get()) {
            throw new IllegalStateException("Remote node " + id + " is closed");
        }

        PooledConnection connection = null;
        try {
            connection = acquireConnection();
            CompletableFuture<T> future = action.apply(connection);
            T result = future.get(requestTimeoutMillis, TimeUnit.MILLISECONDS);
            release(connection);
            return result;
        } catch (TimeoutException e) {
            if (connection != null) {
                discard(connection);
            }
            throw communicationError("Request to node timed out", e);
        } catch (ExecutionException e) {
            if (connection != null) {
                discard(connection);
            }
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException runtime) {
                throw communicationError("Remote command failed", runtime);
            }
            if (cause instanceof Exception exception) {
                throw communicationError("Remote command failed", exception);
            }
            throw communicationError("Remote command failed", cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (connection != null) {
                discard(connection);
            }
            throw communicationError("Interrupted while waiting for remote response", e);
        } catch (IOException e) {
            if (connection != null) {
                discard(connection);
            }
            throw communicationError("Failed to acquire connection", e);
        }
    }

    private IllegalStateException communicationError(String message, Throwable cause)
    {
        return new IllegalStateException(message + " from node " + id + " at " + host + ':' + port, cause);
    }

    private PooledConnection acquireConnection() throws IOException
    {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(requestTimeoutMillis);
        while (true) {
            if (closed.get()) {
                throw new IOException("Remote node is closed");
            }
            PooledConnection pooled = pool.poll();
            if (pooled != null) {
                if (!pooled.closed && !pooled.socket.isClosed()) {
                    pooled.socket.resume();
                    return pooled;
                }
                continue;
            }

            int current = openConnections.get();
            if (current < maxPoolSize) {
                if (openConnections.compareAndSet(current, current + 1)) {
                    try {
                        return createConnection();
                    } catch (IOException e) {
                        openConnections.decrementAndGet();
                        throw e;
                    }
                }
                continue;
            }

            long remaining = deadline - System.nanoTime();
            if (remaining <= 0L) {
                throw new IOException("Timeout acquiring pooled connection");
            }
            long waitMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining));
            try {
                pooled = pool.poll(waitMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for pooled connection", e);
            }
            if (pooled == null) {
                throw new IOException("Timeout acquiring pooled connection");
            }
            if (!pooled.closed && !pooled.socket.isClosed()) {
                pooled.socket.resume();
                return pooled;
            }
        }
    }

    private PooledConnection createConnection() throws IOException
    {
        CompletableFuture<NetSocket> future = new CompletableFuture<>();
        netClient.connect(port, host, ar -> {
            if (ar.succeeded()) {
                future.complete(ar.result());
            } else {
                future.completeExceptionally(ar.cause());
            }
        });

        NetSocket socket;
        try {
            socket = future.get(connectTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while connecting", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof IOException io) {
                throw io;
            }
            throw new IOException("Failed to open connection", cause);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new IOException("Connection timed out", e);
        }

        PooledConnection connection = new PooledConnection(socket);
        allConnections.add(connection);
        socket.pause();
        socket.handler(null);
        socket.exceptionHandler(null);
        socket.closeHandler(v -> {
            connection.closed = true;
            pool.remove(connection);
            allConnections.remove(connection);
            Promise<?> pending = connection.clearInFlight();
            if (pending != null) {
                pending.tryFail(new IOException("Connection closed"));
            }
            openConnections.decrementAndGet();
        });
        return connection;
    }

    private void release(PooledConnection connection)
    {
        if (connection == null) {
            return;
        }
        connection.clearInFlight();
        if (closed.get() || connection.closed || connection.socket.isClosed()) {
            discard(connection);
            return;
        }
        connection.socket.pause();
        if (!pool.offer(connection)) {
            discard(connection);
        }
    }

    private void discard(PooledConnection connection)
    {
        if (connection == null) {
            return;
        }
        connection.clearInFlight();
        pool.remove(connection);
        allConnections.remove(connection);
        if (!connection.closed) {
            connection.closed = true;
            try {
                connection.socket.close();
            } catch (Exception ignored) {
            }
        }
    }

    private <T> CompletableFuture<T> send(PooledConnection connection, Buffer request, ResponseParser<T> parser)
    {
        Promise<T> promise = Promise.promise();
        if (!connection.register(promise)) {
            promise.fail(new IllegalStateException("Connection already in use"));
            return promise.future().toCompletionStage().toCompletableFuture();
        }

        NetSocket socket = connection.socket;
        socket.handler(buffer -> {
            try {
                parser.handle(buffer);
                if (parser.completed()) {
                    promise.tryComplete(parser.result());
                }
            } catch (Exception e) {
                promise.tryFail(e);
            }
        });
        socket.exceptionHandler(promise::tryFail);
        socket.write(request, ar -> {
            if (ar.failed()) {
                promise.tryFail(ar.cause());
            }
        });
        promise.future().onComplete(ar -> {
            connection.clear(promise);
            socket.handler(null);
            socket.exceptionHandler(null);
            parser.reset();
        });
        return promise.future().toCompletionStage().toCompletableFuture();
    }

    private long expiryMillis(Duration ttl)
    {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return 0L;
        }
        return System.currentTimeMillis() + ttl.toMillis();
    }

    @Override
    public void close()
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        PooledConnection pooled;
        while ((pooled = pool.poll()) != null) {
            discard(pooled);
        }
        for (PooledConnection connection : allConnections.toArray(new PooledConnection[0])) {
            discard(connection);
        }
        try {
            netClient.close().toCompletionStage().toCompletableFuture().join();
        } catch (Exception ignored) {
        }
    }

    private interface ResponseParser<T>
    {
        void handle(Buffer buffer) throws IOException;

        boolean completed();

        T result();

        void reset();
    }

    private abstract static class AbstractResponseParser<T> implements ResponseParser<T>
    {
        protected final ByteBufferReader reader = new ByteBufferReader();
        protected boolean complete;
        protected T result;

        @Override
        public void handle(Buffer buffer) throws IOException
        {
            reader.append(buffer);
            parse();
        }

        protected abstract void parse() throws IOException;

        @Override
        public boolean completed()
        {
            return complete;
        }

        @Override
        public T result()
        {
            return result;
        }

        @Override
        public void reset()
        {
            reader.reset();
            complete = false;
            result = null;
        }
    }

    private static final class BooleanResponseParser extends AbstractResponseParser<Boolean>
    {
        private final byte trueByte;
        private final byte falseByte;

        private BooleanResponseParser(byte trueByte, byte falseByte)
        {
            this.trueByte = trueByte;
            this.falseByte = falseByte;
        }

        @Override
        protected void parse() throws IOException
        {
            if (!reader.has(1)) {
                return;
            }
            byte response = reader.readByte();
            if (response == trueByte) {
                result = Boolean.TRUE;
                complete = true;
            } else if (response == falseByte) {
                result = Boolean.FALSE;
                complete = true;
            } else {
                throw new IOException("unexpected boolean response: " + (char) response);
            }
        }
    }

    private static final class ClearResponseParser extends AbstractResponseParser<Void>
    {
        @Override
        protected void parse() throws IOException
        {
            if (!reader.has(1)) {
                return;
            }
            byte response = reader.readByte();
            if (response != RESP_OK) {
                throw new IOException("unexpected response to clear: " + (char) response);
            }
            complete = true;
        }
    }

    private static final class GetResponseParser extends AbstractResponseParser<String>
    {
        private enum State { STATUS, LENGTH, VALUE }

        private State state = State.STATUS;
        private int valueLength;

        @Override
        protected void parse() throws IOException
        {
            while (!complete) {
                switch (state) {
                    case STATUS -> {
                        if (!reader.has(1)) {
                            return;
                        }
                        byte response = reader.readByte();
                        if (response == RESP_MISS) {
                            result = null;
                            complete = true;
                            return;
                        }
                        if (response != RESP_HIT) {
                            throw new IOException("unexpected response to get: " + (char) response);
                        }
                        state = State.LENGTH;
                    }
                    case LENGTH -> {
                        if (!reader.has(4)) {
                            return;
                        }
                        valueLength = reader.readInt();
                        if (valueLength < 0) {
                            throw new IOException("negative value length");
                        }
                        state = State.VALUE;
                    }
                    case VALUE -> {
                        if (!reader.has(valueLength)) {
                            return;
                        }
                        byte[] valueBytes = reader.readBytes(valueLength);
                        result = new String(valueBytes, StandardCharsets.UTF_8);
                        complete = true;
                    }
                }
            }
        }

        @Override
        public void reset()
        {
            super.reset();
            state = State.STATUS;
            valueLength = 0;
        }
    }

    private static final class ByteBufferReader
    {
        private Buffer buffer = Buffer.buffer();
        private int readIndex;

        void append(Buffer chunk)
        {
            buffer.appendBuffer(chunk);
        }

        boolean has(int bytes)
        {
            return buffer.length() - readIndex >= bytes;
        }

        byte readByte()
        {
            byte value = buffer.getByte(readIndex);
            readIndex += 1;
            return value;
        }

        int readInt()
        {
            int value = buffer.getInt(readIndex);
            readIndex += 4;
            return value;
        }

        long readLong()
        {
            long value = buffer.getLong(readIndex);
            readIndex += 8;
            return value;
        }

        byte[] readBytes(int length)
        {
            byte[] data = buffer.getBytes(readIndex, readIndex + length);
            readIndex += length;
            return data;
        }

        void reset()
        {
            buffer = Buffer.buffer();
            readIndex = 0;
        }
    }

    private static final class PooledConnection
    {
        final NetSocket socket;
        private final AtomicReference<Promise<?>> inFlight = new AtomicReference<>();
        volatile boolean closed;

        private PooledConnection(NetSocket socket)
        {
            this.socket = socket;
        }

        boolean register(Promise<?> promise)
        {
            return inFlight.compareAndSet(null, promise);
        }

        void clear(Promise<?> promise)
        {
            inFlight.compareAndSet(promise, null);
        }

        Promise<?> clearInFlight()
        {
            return inFlight.getAndSet(null);
        }
    }
}
