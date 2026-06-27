package com.cancache.agent.cluster.coordination;

import org.jboss.logging.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP soket bağlantılarını havuzlayan ve yeniden kullanan bileşen.
 * Bağlantılar ödünç alınır, kullanılır ve havuza iade edilir.
 * Sağlık kontrolü başarısız olan bağlantılar otomatik olarak kapatılır.
 */
public class SocketConnectionPool implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(SocketConnectionPool.class);

    private final String host;
    private final int port;
    private final int maxPoolSize;
    private final int connectTimeoutMillis;
    private final int socketTimeoutMillis;
    private final long maxIdleTimeMillis;
    private final long acquireTimeoutMillis;

    private final BlockingDeque<PooledSocket> pool;
    private final Semaphore connectionPermits;
    private final AtomicInteger totalConnections = new AtomicInteger(0);
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Yeni bir connection pool oluşturur.
     *
     * @param host                Hedef host
     * @param port                Hedef port
     * @param maxPoolSize         Maksimum havuz boyutu
     * @param connectTimeoutMillis Bağlantı timeout süresi (ms)
     */
    public SocketConnectionPool(String host, int port, int maxPoolSize, int connectTimeoutMillis) {
        this(host, port, maxPoolSize, connectTimeoutMillis, 60_000L, Math.max(1000, connectTimeoutMillis));
    }

    SocketConnectionPool(String host, int port, int maxPoolSize, int connectTimeoutMillis,
                         long maxIdleTimeMillis, long acquireTimeoutMillis) {
        this.host = host;
        this.port = port;
        this.maxPoolSize = Math.max(1, maxPoolSize);
        this.connectTimeoutMillis = Math.max(100, connectTimeoutMillis);
        this.socketTimeoutMillis = Math.max(5000, connectTimeoutMillis * 2);
        this.maxIdleTimeMillis = Math.max(1L, maxIdleTimeMillis);
        this.acquireTimeoutMillis = Math.max(1L, acquireTimeoutMillis);
        this.pool = new LinkedBlockingDeque<>(this.maxPoolSize);
        this.connectionPermits = new Semaphore(this.maxPoolSize, true);
    }

    /**
     * Havuzdan bir bağlantı alır veya yeni bağlantı oluşturur.
     *
     * @return Kullanılabilir PooledSocket
     * @throws IOException Bağlantı kurulamazsa
     */
    public PooledSocket acquire() throws IOException {
        if (closed.get()) {
            throw new IOException("Connection pool is closed");
        }

        if (!acquirePermit()) {
            throw new IOException("Timeout acquiring pooled connection");
        }

        activeConnections.incrementAndGet();
        boolean success = false;
        try {
            if (closed.get()) {
                throw new IOException("Connection pool is closed");
            }
            PooledSocket pooled = acquireIdleConnection();
            if (pooled == null) {
                pooled = createNewConnection();
            }
            if (closed.get()) {
                closeQuietly(pooled);
                decrementTotalConnections();
                throw new IOException("Connection pool is closed");
            }
            pooled.markBorrowed();
            success = true;
            return pooled;
        } finally {
            if (!success) {
                decrementActiveConnections();
                connectionPermits.release();
            }
        }
    }

    /**
     * Bağlantıyı havuza iade eder.
     *
     * @param pooledSocket İade edilecek bağlantı
     */
    public void release(PooledSocket pooledSocket) {
        if (pooledSocket == null) {
            return;
        }

        decrementActiveConnections();

        try {
            if (closed.get() || !isValid(pooledSocket)) {
                closeQuietly(pooledSocket);
                decrementTotalConnections();
                return;
            }

            pooledSocket.markReturned();

            // Havuza geri ekle
            if (!pool.offerFirst(pooledSocket)) {
                // Havuz dolu, bağlantıyı kapat
                closeQuietly(pooledSocket);
                decrementTotalConnections();
            }
        } finally {
            connectionPermits.release();
        }
    }

    /**
     * Bağlantıyı havuza iade etmeden kapatır (hata durumlarında).
     *
     * @param pooledSocket Kapatılacak bağlantı
     */
    public void discard(PooledSocket pooledSocket) {
        if (pooledSocket == null) {
            return;
        }
        decrementActiveConnections();
        try {
            closeQuietly(pooledSocket);
            decrementTotalConnections();
        } finally {
            connectionPermits.release();
        }
    }

    private PooledSocket createNewConnection() throws IOException {
        Socket socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            socket.setSoTimeout(socketTimeoutMillis);
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMillis);

            PooledSocket pooled = new PooledSocket(socket);
            socket = null;
            totalConnections.incrementAndGet();

            LOG.debugf("Created new connection to %s:%d (total: %d, active: %d)",
                    host, port, totalConnections.get(), activeConnections.get());

            return pooled;
        } catch (IOException e) {
            throw e;
        } finally {
            if (socket != null) {
                closeRawSocket(socket);
            }
        }
    }

    private boolean acquirePermit() throws IOException {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMillis);
        while (true) {
            if (closed.get()) {
                throw new IOException("Connection pool is closed");
            }

            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                return false;
            }

            long waitNanos = Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(50));
            try {
                if (connectionPermits.tryAcquire(waitNanos, TimeUnit.NANOSECONDS)) {
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for connection", e);
            }
        }
    }

    private PooledSocket acquireIdleConnection() {
        PooledSocket pooled = pool.pollFirst();
        while (pooled != null) {
            if (isValid(pooled)) {
                return pooled;
            }
            closeQuietly(pooled);
            decrementTotalConnections();
            pooled = pool.pollFirst();
        }
        return null;
    }

    private boolean isValid(PooledSocket pooled) {
        if (pooled == null) {
            return false;
        }

        // Idle timeout kontrolü
        if (System.currentTimeMillis() - pooled.lastUsedTime() > maxIdleTimeMillis) {
            LOG.debugf("Connection expired due to idle timeout: %s:%d", host, port);
            return false;
        }

        // Socket durumu kontrolü
        Socket socket = pooled.socket();
        return socket != null
                && !socket.isClosed()
                && socket.isConnected()
                && !socket.isInputShutdown()
                && !socket.isOutputShutdown();
    }

    private void closeQuietly(PooledSocket pooled) {
        if (pooled != null) {
            try {
                pooled.close();
            } catch (Exception e) {
                LOG.debugf(e, "Error closing pooled connection to %s:%d", host, port);
            }
        }
    }

    private void closeRawSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            LOG.debugf(e, "Error closing socket to %s:%d", host, port);
        }
    }

    private void decrementActiveConnections() {
        activeConnections.updateAndGet(current -> current > 0 ? current - 1 : 0);
    }

    private void decrementTotalConnections() {
        totalConnections.updateAndGet(current -> current > 0 ? current - 1 : 0);
    }

    /**
     * Havuz istatistiklerini döndürür.
     */
    public PoolStats getStats() {
        return new PoolStats(
                totalConnections.get(),
                activeConnections.get(),
                pool.size(),
                maxPoolSize
        );
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        LOG.debugf("Closing connection pool for %s:%d", host, port);

        PooledSocket pooled;
        while ((pooled = pool.pollFirst()) != null) {
            closeQuietly(pooled);
            decrementTotalConnections();
        }
    }

    /**
     * Havuzlanmış soket wrapper'ı.
     */
    public static final class PooledSocket implements AutoCloseable {
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;
        private volatile long lastUsedTime;
        private volatile long borrowedTime;

        PooledSocket(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 8192));
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 8192));
            this.lastUsedTime = System.currentTimeMillis();
        }

        public Socket socket() {
            return socket;
        }

        public DataInputStream in() {
            return in;
        }

        public DataOutputStream out() {
            return out;
        }

        public void flush() throws IOException {
            out.flush();
        }

        void markBorrowed() {
            this.borrowedTime = System.currentTimeMillis();
        }

        void markReturned() {
            this.lastUsedTime = System.currentTimeMillis();
        }

        long lastUsedTime() {
            return lastUsedTime;
        }

        @Override
        public void close() throws IOException {
            try {
                in.close();
            } catch (IOException ignored) {
            }
            try {
                out.close();
            } catch (IOException ignored) {
            }
            socket.close();
        }
    }

    /**
     * Havuz istatistikleri.
     */
    public record PoolStats(
            int totalConnections,
            int activeConnections,
            int idleConnections,
            int maxPoolSize
    ) {
        @Override
        public String toString() {
            return String.format("PoolStats[total=%d, active=%d, idle=%d, max=%d]",
                    totalConnections, activeConnections, idleConnections, maxPoolSize);
        }
    }
}
