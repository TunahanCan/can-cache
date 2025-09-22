package com.can.cache.integration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

final class CanCacheServiceLauncher implements AutoCloseable {

    private final EmbeddedCanCacheServer server;

    private CanCacheServiceLauncher(EmbeddedCanCacheServer server) {
        this.server = server;
    }

    static CanCacheServiceLauncher ensureStarted() throws IOException {
        String host = MemcacheTextClient.DEFAULT_HOST;
        int port = MemcacheTextClient.DEFAULT_PORT;
        if (isServiceReachable(host, port)) {
            return new CanCacheServiceLauncher(null);
        }
        EmbeddedCanCacheServer server = new EmbeddedCanCacheServer(host, port);
        server.start();
        return new CanCacheServiceLauncher(server);
    }

    private static boolean isServiceReachable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 200);
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    @Override
    public void close() {
        if (server != null) {
            server.close();
        }
    }
}
