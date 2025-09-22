package com.can.cache.integration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashSet;

final class CanCacheServiceLauncher implements AutoCloseable {

    private final EmbeddedCanCacheServer server;

    private CanCacheServiceLauncher(EmbeddedCanCacheServer server) {
        this.server = server;
    }

    static CanCacheServiceLauncher ensureStarted() throws IOException {
        String configuredHost = MemcacheTextClient.configuredHost();
        int configuredPort = MemcacheTextClient.configuredPort();
        if (isServiceReachable(configuredHost, configuredPort)) {
            MemcacheTextClient.overrideDefaultEndpoint(configuredHost, configuredPort);
            return new CanCacheServiceLauncher(null);
        }
        EmbeddedCanCacheServer server = startEmbeddedServer(configuredHost, configuredPort);
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

    private static EmbeddedCanCacheServer startEmbeddedServer(String configuredHost, int port) throws IOException {
        IOException lastError = null;
        for (String candidate : candidateHosts(configuredHost)) {
            EmbeddedCanCacheServer server = new EmbeddedCanCacheServer(candidate, port);
            try {
                server.start();
                MemcacheTextClient.overrideDefaultEndpoint(candidate, port);
                return server;
            } catch (IOException ex) {
                server.close();
                lastError = ex;
            }
        }
        if (lastError != null) {
            throw lastError;
        }
        throw new IOException("Unable to start embedded server for host " + configuredHost + ':' + port);
    }

    private static Iterable<String> candidateHosts(String configuredHost) {
        LinkedHashSet<String> hosts = new LinkedHashSet<>();
        hosts.add(configuredHost);
        hosts.add(MemcacheTextClient.loopbackHost());
        return hosts;
    }

    @Override
    public void close() {
        if (server != null) {
            server.close();
        }
    }
}
