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
        this.host = host;
        this.port = port;
        this.maxPoolSize = Math.max(1, maxPoolSize);
        this.connectTimeoutMillis = Math.max(100, connectTimeoutMillis);
        this.socketTimeoutMillis = Math.max(5000, connectTimeoutMillis * 2);
        this.maxIdleTimeMillis = 60_000L; // 1 dakika idle timeout
        this.acquireTimeoutMillis = Math.max(1000, connectTimeoutMillis);
        this.pool = new LinkedBlockingDeque<>(this.maxPoolSize);
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

        // Önce havuzdan almayı dene
        PooledSocket pooled = pool.pollFirst();
        while (pooled != null) {
            if (isValid(pooled)) {
                activeConnections.incrementAndGet();
                pooled.markBorrowed();
                return pooled;
            }
            // Geçersiz bağlantıyı kapat
            closeQuietly(pooled);
            totalConnections.decrementAndGet();
            pooled = pool.pollFirst();
        }

        // Havuzda uygun bağlantı yok, yeni oluştur
        if (totalConnections.get() < maxPoolSize) {
            return createNewConnection();
        }

        // Maksimum kapasitede, havuzdan bağlantı bekle
        try {
            pooled = pool.pollFirst(acquireTimeoutMillis, TimeUnit.MILLISECONDS);
            if (pooled != null && isValid(pooled)) {
                activeConnections.incrementAndGet();
                pooled.markBorrowed();
                return pooled;
            }
            if (pooled != null) {
                closeQuietly(pooled);
                totalConnections.decrementAndGet();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for connection", e);
        }

        // Son çare: yeni bağlantı oluşturmayı zorla
        return createNewConnection();
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

        activeConnections.decrementAndGet();

        if (closed.get() || !isValid(pooledSocket)) {
            closeQuietly(pooledSocket);
            totalConnections.decrementAndGet();
            return;
        }

        pooledSocket.markReturned();

        // Havuza geri ekle
        if (!pool.offerFirst(pooledSocket)) {
            // Havuz dolu, bağlantıyı kapat
            closeQuietly(pooledSocket);
            totalConnections.decrementAndGet();
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
        activeConnections.decrementAndGet();
        closeQuietly(pooledSocket);
        totalConnections.decrementAndGet();
    }

    private PooledSocket createNewConnection() throws IOException {
        totalConnections.incrementAndGet();
        activeConnections.incrementAndGet();

        try {
            Socket socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            socket.setSoTimeout(socketTimeoutMillis);
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMillis);

            PooledSocket pooled = new PooledSocket(socket);
            pooled.markBorrowed();

            LOG.debugf("Created new connection to %s:%d (total: %d, active: %d)",
                    host, port, totalConnections.get(), activeConnections.get());

            return pooled;
        } catch (IOException e) {
            totalConnections.decrementAndGet();
            activeConnections.decrementAndGet();
            throw e;
        }
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
        }

        totalConnections.set(0);
        activeConnections.set(0);
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
