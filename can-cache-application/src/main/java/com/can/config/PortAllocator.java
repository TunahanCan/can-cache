package com.can.config;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * Provides runtime resolution for network ports that may already be occupied
 * when multiple application instances are started on the same host. The
 * allocator checks whether the requested port from configuration is available
 * and falls back to an automatically assigned free port when necessary.
 */
@Singleton
public class PortAllocator
{
    private static final Logger LOG = Logger.getLogger(PortAllocator.class);

    private final int networkPort;
    private final int replicationPort;
    private final int metricsPort;

    @Inject
    public PortAllocator(AppProperties properties)
    {
        var network = properties.network();
        this.networkPort = resolveTcpPort(network.host(), network.port(), "cancached server");

        var replication = properties.cluster().replication();
        this.replicationPort = resolveTcpPort(replication.bindHost(), replication.port(), "replication server");

        var metrics = properties.metrics();
        if (metrics.endpointEnabled()) {
            this.metricsPort = resolveTcpPort(metrics.endpointHost(), metrics.endpointPort(), "metrics endpoint");
        }
        else {
            this.metricsPort = metrics.endpointPort();
        }
    }

    public int networkPort()
    {
        return networkPort;
    }

    public int replicationPort()
    {
        return replicationPort;
    }

    public int metricsPort()
    {
        return metricsPort;
    }

    private int resolveTcpPort(String host, int requestedPort, String componentName)
    {
        if (requestedPort <= 0) {
            return findFreeTcpPort(host);
        }
        if (isPortAvailable(host, requestedPort)) {
            return requestedPort;
        }
        int fallback = findFreeTcpPort(host);
        LOG.warnf("Port %d for %s on host %s is already in use, falling back to %d", requestedPort, componentName,
                hostOrWildcard(host), fallback);
        return fallback;
    }

    private boolean isPortAvailable(String host, int port)
    {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(socketAddress(host, port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private int findFreeTcpPort(String host)
    {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(socketAddress(host, 0));
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to allocate free TCP port", e);
        }
    }

    private InetSocketAddress socketAddress(String host, int port)
    {
        if (host == null || host.isBlank()) {
            return new InetSocketAddress(port);
        }
        return new InetSocketAddress(host, port);
    }

    private String hostOrWildcard(String host)
    {
        if (host == null || host.isBlank()) {
            return "0.0.0.0";
        }
        return host;
    }
}
