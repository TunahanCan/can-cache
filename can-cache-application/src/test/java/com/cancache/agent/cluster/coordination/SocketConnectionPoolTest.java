package com.cancache.agent.cluster.coordination;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SocketConnectionPoolTest
{
    @Test
    void shouldNotOpenMoreThanConfiguredPoolSize() throws Exception
    {
        try (LoopbackServer server = new LoopbackServer();
             SocketConnectionPool pool = new SocketConnectionPool("127.0.0.1", server.port(), 1,
                     100, TimeUnit.MINUTES.toMillis(1), 100)) {
            SocketConnectionPool.PooledSocket first = pool.acquire();
            try {
                IOException error = assertThrows(IOException.class, pool::acquire);

                assertTrue(error.getMessage().contains("Timeout acquiring pooled connection"));
                assertEquals(1, pool.getStats().totalConnections());
                assertEquals(1, pool.getStats().activeConnections());
            } finally {
                pool.discard(first);
            }
        }
    }

    @Test
    void shouldKeepStatsNonNegativeWhenBorrowedConnectionReturnsAfterClose() throws Exception
    {
        try (LoopbackServer server = new LoopbackServer();
             SocketConnectionPool pool = new SocketConnectionPool("127.0.0.1", server.port(), 1,
                     100, TimeUnit.MINUTES.toMillis(1), 100)) {
            SocketConnectionPool.PooledSocket first = pool.acquire();

            pool.close();
            pool.release(first);

            assertEquals(0, pool.getStats().totalConnections());
            assertEquals(0, pool.getStats().activeConnections());
            assertEquals(0, pool.getStats().idleConnections());
        }
    }

    private static final class LoopbackServer implements AutoCloseable
    {
        private final ServerSocket serverSocket;
        private final List<Socket> acceptedSockets = new CopyOnWriteArrayList<>();
        private final Thread acceptThread;

        private LoopbackServer() throws IOException
        {
            this.serverSocket = new ServerSocket(0);
            this.acceptThread = Thread.ofVirtual().start(this::acceptLoop);
        }

        private int port()
        {
            return serverSocket.getLocalPort();
        }

        private void acceptLoop()
        {
            while (!serverSocket.isClosed()) {
                try {
                    acceptedSockets.add(serverSocket.accept());
                } catch (IOException ignored) {
                    return;
                }
            }
        }

        @Override
        public void close() throws Exception
        {
            serverSocket.close();
            for (Socket socket : acceptedSockets) {
                socket.close();
            }
            acceptThread.join(TimeUnit.SECONDS.toMillis(1));
        }
    }
}
