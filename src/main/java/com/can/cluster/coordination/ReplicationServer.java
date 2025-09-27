package com.can.cluster.coordination;

import com.can.cluster.ClusterState;
import com.can.config.AppProperties;
import com.can.constants.NodeProtocol;
import com.can.core.CacheEngine;
import io.quarkus.runtime.Startup;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Diğer düğümlerden gelen replikasyon komutlarını kabul ederek {@link CacheEngine}
 * üzerinde uygulayan Vert.x tabanlı TCP sunucusudur. Komut protokolü, {@link RemoteNode}
 * tarafından kullanılan tek baytlık mesajlardan oluşur ve her bağlantı üzerinde
 * gelen istekler sıralı olarak işlenir. Ağ işlemleri Vert.x event-loop'larında
 * yürütülürken, önbellek operasyonları paylaşılan worker havuzu üzerinden
 * çalıştırılarak bloklama engellenir.
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
    private final Vertx vertx;

    private volatile boolean running;
    private NetServer netServer;
    private final Set<ReplicationConnection> connections = ConcurrentHashMap.newKeySet();

    @Inject
    public ReplicationServer(CacheEngine<String, String> engine,
                             ClusterState clusterState,
                             AppProperties properties,
                             WorkerExecutor workerExecutor,
                             Vertx vertx)
    {
        this.engine = engine;
        this.clusterState = clusterState;
        this.config = properties.cluster().replication();
        this.workerExecutor = workerExecutor;
        this.vertx = vertx;
    }

    @PostConstruct
    void start()
    {
        NetServerOptions options = new NetServerOptions()
                .setHost(config.bindHost())
                .setPort(config.port())
                .setTcpNoDelay(true)
                .setReuseAddress(true);

        netServer = vertx.createNetServer(options);
        netServer.connectHandler(this::onClientConnected);
        try {
            netServer.listen().toCompletionStage().toCompletableFuture().join();
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to bind replication port", e);
        }

        running = true;
        LOG.infof("Replication server listening on %s:%d (advertised as %s:%d)",
                config.bindHost(), netServer.actualPort(), config.advertiseHost(), config.port());
    }

    private void onClientConnected(NetSocket socket)
    {
        if (!running) {
            socket.close();
            return;
        }

        ReplicationConnection connection = new ReplicationConnection(socket);
        connections.add(connection);
        socket.closeHandler(v -> {
            connection.onClosed();
            connections.remove(connection);
        });
        socket.exceptionHandler(e -> {
            if (LOG.isDebugEnabled()) {
                LOG.debugf(e, "Replication client %s disconnected with error", socket.remoteAddress());
            }
            socket.close();
        });
        socket.handler(connection::handleData);
    }

    @PreDestroy
    @Override
    public void close()
    {
        running = false;
        for (ReplicationConnection connection : connections) {
            connection.closeSilently();
        }
        connections.clear();
        if (netServer != null) {
            try {
                netServer.close().toCompletionStage().toCompletableFuture().join();
            } catch (Exception ignored) {
            }
        }
    }

    private final class ReplicationConnection
    {
        private final NetSocket socket;
        private final ByteBufferReader reader = new ByteBufferReader();

        private boolean closed;
        private boolean processing;
        private CommandDecoder decoder;

        private ReplicationConnection(NetSocket socket)
        {
            this.socket = socket;
        }

        private void handleData(Buffer buffer)
        {
            if (closed) {
                return;
            }
            reader.append(buffer);
            processBuffer();
        }

        private void processBuffer()
        {
            while (!closed && !processing) {
                if (decoder == null) {
                    if (!reader.has(1)) {
                        return;
                    }
                    byte command = reader.readByte();
                    decoder = decoderFor(command);
                    if (decoder == null) {
                        LOG.warnf("Unknown replication command %d from %s", command & 0xff, socket.remoteAddress());
                        close();
                        return;
                    }
                }

                try {
                    CommandAction action = decoder.tryDecode(reader);
                    if (action == null) {
                        return;
                    }
                    decoder = null;
                    reader.compact();
                    executeCommand(action);
                    return;
                } catch (IOException e) {
                    LOG.debugf(e, "Failed to decode replication command from %s", socket.remoteAddress());
                    close();
                    return;
                }
            }
        }

        private void executeCommand(CommandAction action)
        {
            processing = true;
            workerExecutor.<Buffer>executeBlocking(() -> action.execute(), false).onComplete(ar -> {
                processing = false;
                if (closed) {
                    return;
                }
                if (ar.succeeded()) {
                    Buffer response = ar.result();
                    if (response != null && response.length() > 0) {
                        socket.write(response);
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debugf(ar.cause(), "Replication client %s disconnected with error", socket.remoteAddress());
                    }
                    close();
                    return;
                }
                processBuffer();
            });
        }

        private CommandDecoder decoderFor(byte command)
        {
            return switch (command) {
                case NodeProtocol.CMD_SET -> new SetCommandDecoder();
                case NodeProtocol.CMD_GET -> new GetCommandDecoder();
                case NodeProtocol.CMD_DELETE -> new DeleteCommandDecoder();
                case NodeProtocol.CMD_CLEAR -> new ClearCommandDecoder();
                case NodeProtocol.CMD_CAS -> new CasCommandDecoder();
                case NodeProtocol.CMD_JOIN -> new JoinCommandDecoder();
                case NodeProtocol.CMD_STREAM -> new StreamCommandDecoder();
                case NodeProtocol.CMD_DIGEST -> new DigestCommandDecoder();
                default -> null;
            };
        }

        private void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            decoder = null;
            reader.reset();
            socket.close();
        }

        private void closeSilently()
        {
            if (closed) {
                return;
            }
            closed = true;
            decoder = null;
            reader.reset();
            socket.close();
        }

        private void onClosed()
        {
            closed = true;
            decoder = null;
            processing = false;
            reader.reset();
        }

        private abstract static class BaseCommandDecoder implements CommandDecoder
        {
            protected void ensureLength(int length) throws IOException
            {
                if (length < 0) {
                    throw new IOException("negative length in replication command");
                }
            }
        }

        private final class SetCommandDecoder extends BaseCommandDecoder
        {
            private enum Stage { HEADER, KEY, VALUE }

            private Stage stage = Stage.HEADER;
            private int keyLength;
            private int valueLength;
            private long expireAt;
            private byte[] keyBytes;
            private byte[] valueBytes;

            @Override
            public CommandAction tryDecode(ByteBufferReader reader) throws IOException
            {
                while (true) {
                    switch (stage) {
                        case HEADER -> {
                            if (!reader.has(4 + 4 + 8)) {
                                return null;
                            }
                            keyLength = reader.readInt();
                            valueLength = reader.readInt();
                            expireAt = reader.readLong();
                            ensureLength(keyLength);
                            ensureLength(valueLength);
                            stage = Stage.KEY;
                        }
                        case KEY -> {
                            if (!reader.has(keyLength)) {
                                return null;
                            }
                            keyBytes = reader.readBytes(keyLength);
                            stage = Stage.VALUE;
                        }
                        case VALUE -> {
                            if (!reader.has(valueLength)) {
                                return null;
                            }
                            valueBytes = reader.readBytes(valueLength);
                            return () -> handleSet(keyBytes, valueBytes, expireAt);
                        }
                    }
                }
            }
        }

        private final class GetCommandDecoder extends BaseCommandDecoder
        {
            private enum Stage { LENGTH, KEY }

            private Stage stage = Stage.LENGTH;
            private int keyLength;
            private byte[] keyBytes;

            @Override
            public CommandAction tryDecode(ByteBufferReader reader) throws IOException
            {
                while (true) {
                    switch (stage) {
                        case LENGTH -> {
                            if (!reader.has(4)) {
                                return null;
                            }
                            keyLength = reader.readInt();
                            ensureLength(keyLength);
                            stage = Stage.KEY;
                        }
                        case KEY -> {
                            if (!reader.has(keyLength)) {
                                return null;
                            }
                            keyBytes = reader.readBytes(keyLength);
                            return () -> handleGet(keyBytes);
                        }
                    }
                }
            }
        }

        private final class DeleteCommandDecoder extends BaseCommandDecoder
        {
            private enum Stage { LENGTH, KEY }

            private Stage stage = Stage.LENGTH;
            private int keyLength;
            private byte[] keyBytes;

            @Override
            public CommandAction tryDecode(ByteBufferReader reader) throws IOException
            {
                while (true) {
                    switch (stage) {
                        case LENGTH -> {
                            if (!reader.has(4)) {
                                return null;
                            }
                            keyLength = reader.readInt();
                            ensureLength(keyLength);
                            stage = Stage.KEY;
                        }
                        case KEY -> {
                            if (!reader.has(keyLength)) {
                                return null;
                            }
                            keyBytes = reader.readBytes(keyLength);
                            return () -> handleDelete(keyBytes);
                        }
                    }
                }
            }
        }

        private final class ClearCommandDecoder implements CommandDecoder
        {
            @Override
            public CommandAction tryDecode(ByteBufferReader reader)
            {
                return ReplicationConnection.this::handleClear;
            }
        }

        private final class CasCommandDecoder extends BaseCommandDecoder
        {
            private enum Stage { HEADER, KEY, VALUE }

            private Stage stage = Stage.HEADER;
            private int keyLength;
            private int valueLength;
            private long expireAt;
            private long expectedCas;
            private byte[] keyBytes;
            private byte[] valueBytes;

            @Override
            public CommandAction tryDecode(ByteBufferReader reader) throws IOException
            {
                while (true) {
                    switch (stage) {
                        case HEADER -> {
                            if (!reader.has(4 + 4 + 8 + 8)) {
                                return null;
                            }
                            keyLength = reader.readInt();
                            valueLength = reader.readInt();
                            expireAt = reader.readLong();
                            expectedCas = reader.readLong();
                            ensureLength(keyLength);
                            ensureLength(valueLength);
                            stage = Stage.KEY;
                        }
                        case KEY -> {
                            if (!reader.has(keyLength)) {
                                return null;
                            }
                            keyBytes = reader.readBytes(keyLength);
                            stage = Stage.VALUE;
                        }
                        case VALUE -> {
                            if (!reader.has(valueLength)) {
                                return null;
                            }
                            valueBytes = reader.readBytes(valueLength);
                            return () -> handleCas(keyBytes, valueBytes, expireAt, expectedCas);
                        }
                    }
                }
            }
        }

        private final class JoinCommandDecoder extends BaseCommandDecoder
        {
            private enum Stage { LENGTH, ID, EPOCH }

            private Stage stage = Stage.LENGTH;
            private int idLength;
            private byte[] idBytes;
            private long epoch;

            @Override
            public CommandAction tryDecode(ByteBufferReader reader) throws IOException
            {
                while (true) {
                    switch (stage) {
                        case LENGTH -> {
                            if (!reader.has(4)) {
                                return null;
                            }
                            idLength = reader.readInt();
                            ensureLength(idLength);
                            stage = Stage.ID;
                        }
                        case ID -> {
                            if (!reader.has(idLength)) {
                                return null;
                            }
                            idBytes = reader.readBytes(idLength);
                            stage = Stage.EPOCH;
                        }
                        case EPOCH -> {
                            if (!reader.has(8)) {
                                return null;
                            }
                            epoch = reader.readLong();
                            return () -> handleJoin(idBytes, epoch);
                        }
                    }
                }
            }
        }

        private final class StreamCommandDecoder implements CommandDecoder
        {
            @Override
            public CommandAction tryDecode(ByteBufferReader reader)
            {
                return ReplicationConnection.this::handleStream;
            }
        }

        private final class DigestCommandDecoder implements CommandDecoder
        {
            @Override
            public CommandAction tryDecode(ByteBufferReader reader)
            {
                return ReplicationConnection.this::handleDigest;
            }
        }

        private Buffer handleSet(byte[] keyBytes, byte[] valueBytes, long expireAt)
        {
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

            return Buffer.buffer(1).appendByte(stored ? NodeProtocol.RESP_TRUE : NodeProtocol.RESP_FALSE);
        }

        private Buffer handleGet(byte[] keyBytes)
        {
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            String value = engine.get(key);
            if (value == null) {
                return Buffer.buffer(1).appendByte(NodeProtocol.RESP_MISS);
            }
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            return Buffer.buffer(1 + 4 + valueBytes.length)
                    .appendByte(NodeProtocol.RESP_HIT)
                    .appendInt(valueBytes.length)
                    .appendBytes(valueBytes);
        }

        private Buffer handleDelete(byte[] keyBytes)
        {
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            boolean removed = engine.delete(key);
            return Buffer.buffer(1).appendByte(removed ? NodeProtocol.RESP_TRUE : NodeProtocol.RESP_FALSE);
        }

        private Buffer handleClear()
        {
            engine.clear();
            return Buffer.buffer(1).appendByte(NodeProtocol.RESP_OK);
        }

        private Buffer handleCas(byte[] keyBytes, byte[] valueBytes, long expireAt, long expectedCas)
        {
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

            return Buffer.buffer(1).appendByte(stored ? NodeProtocol.RESP_TRUE : NodeProtocol.RESP_FALSE);
        }

        private Buffer handleJoin(byte[] joinerIdBytes, long joinerEpoch)
        {
            clusterState.observeEpoch(joinerEpoch);
            String joinerId = new String(joinerIdBytes, StandardCharsets.UTF_8);
            if (Objects.equals(joinerId, clusterState.localNodeId())) {
                return Buffer.buffer(1).appendByte(NodeProtocol.RESP_REJECT);
            }

            byte[] idBytes = clusterState.localNodeIdBytes();
            return Buffer.buffer(1 + 4 + idBytes.length + 8)
                    .appendByte(NodeProtocol.RESP_ACCEPT)
                    .appendInt(idBytes.length)
                    .appendBytes(idBytes)
                    .appendLong(clusterState.currentEpoch());
        }

        private Buffer handleStream() throws IOException
        {
            try {
                engine.forEachEntry((key, value, expireAt) -> {
                    try {
                        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                        Buffer chunk = Buffer.buffer(1 + 4 + 4 + 8 + keyBytes.length + value.length);
                        chunk.appendByte(NodeProtocol.STREAM_CHUNK_MARKER);
                        chunk.appendInt(keyBytes.length);
                        chunk.appendInt(value.length);
                        chunk.appendLong(expireAt);
                        chunk.appendBytes(keyBytes);
                        chunk.appendBytes(value);
                        socket.write(chunk);
                    } catch (Exception e) {
                        IOException io = e instanceof IOException ? (IOException) e : new IOException(e);
                        throw new StreamWriteException(io);
                    }
                });
            } catch (StreamWriteException e) {
                throw e.unwrap();
            }
            return Buffer.buffer(1).appendByte(NodeProtocol.STREAM_END_MARKER);
        }

        private Buffer handleDigest()
        {
            return Buffer.buffer(8).appendLong(engine.fingerprint());
        }
    }

    private interface CommandDecoder
    {
        CommandAction tryDecode(ByteBufferReader reader) throws IOException;
    }

    @FunctionalInterface
    private interface CommandAction
    {
        Buffer execute() throws Exception;
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

        void compact()
        {
            if (readIndex == 0) {
                return;
            }
            if (readIndex >= buffer.length()) {
                buffer = Buffer.buffer();
                readIndex = 0;
            } else {
                buffer = buffer.getBuffer(readIndex, buffer.length());
                readIndex = 0;
            }
        }

        void reset()
        {
            buffer = Buffer.buffer();
            readIndex = 0;
        }
    }

    private static final class StreamWriteException extends RuntimeException
    {
        private final IOException cause;

        private StreamWriteException(IOException cause)
        {
            super(cause);
            this.cause = cause;
        }

        private IOException unwrap()
        {
            return cause;
        }
    }
}
