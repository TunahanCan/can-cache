package com.can.config;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final int MAX_PORT = 65_535;
    private static final Pattern TRAILING_NUMBER = Pattern.compile("(.*?)(\\d+)$");

    private final ResolvedEndpoint networkEndpoint;
    private final ResolvedEndpoint replicationEndpoint;
    private final ResolvedEndpoint metricsEndpoint;
    private final String replicationAdvertiseHost;

    @Inject
    public PortAllocator(AppProperties properties)
    {
        var network = properties.network();
        this.networkEndpoint = resolveEndpoint(network.host(), network.port(), "cancached server",
                allowHostIncrement(network.host()));

        var replication = properties.cluster().replication();
        this.replicationEndpoint = resolveEndpoint(replication.bindHost(), replication.port(), "replication server",
                allowHostIncrement(replication.bindHost()));
        this.replicationAdvertiseHost = resolveAdvertiseHost(replication.advertiseHost(), replication.bindHost(),
                replicationEndpoint.host());

        var metrics = properties.metrics();
        if (metrics.endpointEnabled()) {
            this.metricsEndpoint = resolveEndpoint(metrics.endpointHost(), metrics.endpointPort(), "metrics endpoint",
                    allowHostIncrement(metrics.endpointHost()));
        }
        else {
            this.metricsEndpoint = new ResolvedEndpoint(metrics.endpointHost(), metrics.endpointPort());
        }
    }

    public String networkHost()
    {
        return networkEndpoint.host();
    }

    public int networkPort()
    {
        return networkEndpoint.port();
    }

    public String replicationHost()
    {
        return replicationEndpoint.host();
    }

    public int replicationPort()
    {
        return replicationEndpoint.port();
    }

    public String replicationAdvertiseHost()
    {
        return replicationAdvertiseHost;
    }

    public String metricsHost()
    {
        return metricsEndpoint.host();
    }

    public int metricsPort()
    {
        return metricsEndpoint.port();
    }

    private ResolvedEndpoint resolveEndpoint(String host, int requestedPort, String componentName, boolean incrementHost)
    {
        if (requestedPort <= 0) {
            return new ResolvedEndpoint(host, findFreeTcpPort(host));
        }
        String candidateHost = host;
        int candidatePort = requestedPort;
        boolean loggedConflict = false;
        while (candidatePort > 0 && candidatePort <= MAX_PORT) {
            if (isPortAvailable(candidateHost, candidatePort)) {
                if (loggedConflict || candidatePort != requestedPort || !Objects.equals(candidateHost, host)) {
                    LOG.warnf("Port %d for %s on host %s is already in use, falling back to %s:%d",
                            requestedPort, componentName, hostOrWildcard(host), hostOrWildcard(candidateHost),
                            candidatePort);
                }
                return new ResolvedEndpoint(candidateHost, candidatePort);
            }
            if (!loggedConflict) {
                LOG.warnf("Port %d for %s on host %s is already in use, trying next host/port combination",
                        requestedPort, componentName, hostOrWildcard(host));
                loggedConflict = true;
            }
            if (candidatePort == MAX_PORT) {
                break;
            }
            ResolvedEndpoint next = nextEndpoint(candidateHost, candidatePort, incrementHost);
            if (next.port() == candidatePort && Objects.equals(next.host(), candidateHost)) {
                break;
            }
            candidateHost = next.host();
            candidatePort = next.port();
        }
        throw new IllegalStateException("No available port for " + componentName + " after checking from "
                + requestedPort + " to " + MAX_PORT);
    }

    private ResolvedEndpoint nextEndpoint(String host, int port, boolean incrementHost)
    {
        int nextPort = Math.min(MAX_PORT, port + 1);
        String nextHost = host;
        if (incrementHost) {
            String updatedHost = incrementHostValue(host);
            if (!Objects.equals(updatedHost, host)) {
                nextHost = updatedHost;
            }
        }
        return new ResolvedEndpoint(nextHost, nextPort);
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

    private String resolveAdvertiseHost(String configuredAdvertiseHost, String originalBindHost, String resolvedBindHost)
    {
        if (isWildcardHost(configuredAdvertiseHost)) {
            if (isWildcardHost(resolvedBindHost)) {
                return InetAddress.getLoopbackAddress().getHostAddress();
            }
            return resolvedBindHost;
        }
        if (Objects.equals(configuredAdvertiseHost, originalBindHost)) {
            return resolvedBindHost;
        }
        return configuredAdvertiseHost;
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

    private boolean allowHostIncrement(String host)
    {
        return !isWildcardHost(host);
    }

    private boolean isWildcardHost(String host)
    {
        if (host == null) {
            return true;
        }
        String trimmed = host.trim();
        return trimmed.isEmpty() || "0.0.0.0".equals(trimmed) || "::".equals(trimmed);
    }

    private String incrementHostValue(String host)
    {
        if (host == null || host.isBlank()) {
            return host;
        }
        if ("0.0.0.0".equals(host.trim())) {
            return host;
        }
        String ipv4 = incrementIpv4(host);
        if (ipv4 != null) {
            return ipv4;
        }
        Matcher matcher = TRAILING_NUMBER.matcher(host);
        if (matcher.matches()) {
            String prefix = matcher.group(1);
            String digits = matcher.group(2);
            long number = Long.parseLong(digits);
            String incremented = String.format("%0" + digits.length() + "d", number + 1);
            return prefix + incremented;
        }
        return host;
    }

    private String incrementIpv4(String host)
    {
        String[] parts = host.split("\\.");
        if (parts.length != 4) {
            return null;
        }
        int[] values = new int[4];
        for (int i = 0; i < 4; i++) {
            try {
                values[i] = Integer.parseInt(parts[i]);
            } catch (NumberFormatException e) {
                return null;
            }
            if (values[i] < 0 || values[i] > 255) {
                return null;
            }
        }
        int combined = ((values[0] & 0xff) << 24) | ((values[1] & 0xff) << 16)
                | ((values[2] & 0xff) << 8) | (values[3] & 0xff);
        if (combined == 0xffffffff) {
            return host;
        }
        combined += 1;
        int a = (combined >> 24) & 0xff;
        int b = (combined >> 16) & 0xff;
        int c = (combined >> 8) & 0xff;
        int d = combined & 0xff;
        return a + "." + b + "." + c + "." + d;
    }

    private record ResolvedEndpoint(String host, int port)
    {
        private ResolvedEndpoint
        {
            if (port < 0 || port > MAX_PORT) {
                throw new IllegalArgumentException("Port out of range: " + port);
            }
        }
    }
}
