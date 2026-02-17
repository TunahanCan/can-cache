package com.cancache.agent.cluster.coordination;

import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Remote node'lara olan connection pool'ları merkezi olarak yöneten bileşen.
 * Her host:port kombinasyonu için tek bir pool tutulur ve paylaşılır.
 */
@Singleton
public class ConnectionPoolManager implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(ConnectionPoolManager.class);

    private static final int DEFAULT_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 3000;

    private final Map<String, SocketConnectionPool> pools = new ConcurrentHashMap<>();
    private final int defaultPoolSize;
    private final int defaultConnectTimeoutMillis;
    private volatile boolean closed = false;

    public ConnectionPoolManager() {
        this(DEFAULT_POOL_SIZE,
                DEFAULT_CONNECT_TIMEOUT_MS);
    }

    public ConnectionPoolManager(int defaultPoolSize, int defaultConnectTimeoutMillis) {
        this.defaultPoolSize = Math.max(1, defaultPoolSize);
        this.defaultConnectTimeoutMillis = Math.max(100, defaultConnectTimeoutMillis);
        LOG.infof("ConnectionPoolManager initialized with poolSize=%d, connectTimeout=%dms",
                this.defaultPoolSize, this.defaultConnectTimeoutMillis);
    }

    /**
     * Belirtilen host:port için connection pool döndürür.
     * Pool yoksa yeni oluşturulur.
     */
    public SocketConnectionPool getPool(String host, int port)
    {
        if (closed) {
            throw new IllegalStateException("ConnectionPoolManager is closed");
        }

        String key = poolKey(host, port);
        return pools.computeIfAbsent(key, _ -> {
            LOG.debugf("Creating new connection pool for %s:%d", host, port);
            return new SocketConnectionPool(host, port, defaultPoolSize, defaultConnectTimeoutMillis);
        });
    }


    /**
     * Havuzdan bağlantı alır (kısa yol metodu).
     */
    public SocketConnectionPool.PooledSocket acquire(String host, int port) throws IOException {
        return getPool(host, port).acquire();
    }

    /**
     * Bağlantıyı havuza iade eder (kısa yol metodu).
     */
    public void release(String host, int port, SocketConnectionPool.PooledSocket socket) {
        SocketConnectionPool pool = pools.get(poolKey(host, port));
        if (pool != null) {
            pool.release(socket);
        } else if (socket != null) {
            try {
                socket.close();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Bağlantıyı havuza iade etmeden kapatır (hata durumlarında).
     */
    public void discard(String host, int port, SocketConnectionPool.PooledSocket socket) {
        SocketConnectionPool pool = pools.get(poolKey(host, port));
        if (pool != null) {
            pool.discard(socket);
        } else if (socket != null) {
            try {
                socket.close();
            } catch (Exception ignored) {
            }
        }
    }

    private String poolKey(String host, int port) {
        return host + ":" + port;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        LOG.info("Closing ConnectionPoolManager...");

        pools.forEach((key, pool) -> {
            try {
                pool.close();
            } catch (Exception e) {
                LOG.debugf(e, "Error closing pool %s", key);
            }
        });
        pools.clear();

        LOG.info("ConnectionPoolManager closed");
    }
}
