package com.can.config;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
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
        String networkHost = normalizeHost(network.host());
        this.networkEndpoint = resolveEndpoint(networkHost, network.port(), "cancached server",
                allowHostIncrement(networkHost), network.host());

        var replication = properties.cluster().replication();
        String replicationHost = normalizeHost(replication.bindHost());
        this.replicationEndpoint = resolveEndpoint(replicationHost, replication.port(), "replication server",
                allowHostIncrement(replicationHost), replication.bindHost());
        this.replicationAdvertiseHost = resolveAdvertiseHost(replication.advertiseHost(), replication.bindHost(),
                replicationEndpoint.host());

        var metrics = properties.metrics();
        if (metrics.endpointEnabled()) {
            String metricsHost = normalizeHost(metrics.endpointHost());
            this.metricsEndpoint = resolveEndpoint(metricsHost, metrics.endpointPort(), "metrics endpoint",
                    allowHostIncrement(metricsHost), metrics.endpointHost());
        }
        else {
            this.metricsEndpoint = new ResolvedEndpoint(normalizeHost(metrics.endpointHost()), metrics.endpointPort());
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

    private ResolvedEndpoint resolveEndpoint(String normalizedHost,
                                             int requestedPort,
                                             String componentName,
                                             boolean incrementHost,
                                             String originalHost)
    {
        if (requestedPort <= 0) {
            return new ResolvedEndpoint(normalizedHost, findFreeTcpPort(normalizedHost));
        }
        String candidateHost = normalizedHost;
        int candidatePort = requestedPort;
        boolean loggedConflict = false;
        while (candidatePort > 0 && candidatePort <= MAX_PORT) {
            if (isPortAvailable(candidateHost, candidatePort)) {
                if (loggedConflict || candidatePort != requestedPort || !Objects.equals(candidateHost, normalizedHost)) {
                    LOG.warnf("Port %d for %s on host %s is already in use, falling back to %s:%d",
                            requestedPort, componentName, hostOrWildcard(originalHost), hostOrWildcard(candidateHost),
                            candidatePort);
                }
                return new ResolvedEndpoint(candidateHost, candidatePort);
            }
            if (!loggedConflict) {
                LOG.warnf("Port %d for %s on host %s is already in use, trying next host/port combination",
                        requestedPort, componentName, hostOrWildcard(originalHost));
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
        for (SocketCandidate candidate : candidateSocketAddresses(host, port)) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.bind(candidate.address());
            } catch (IOException e) {
                if (!candidate.required() && !(e instanceof java.net.BindException)) {
                    continue;
                }
                return false;
            }
        }
        return true;
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
        String advertiseHost = normalizeHost(configuredAdvertiseHost);
        String bindHost = normalizeHost(originalBindHost);
        String resolvedHost = normalizeHost(resolvedBindHost);

        if (isWildcardHost(advertiseHost)) {
            if (isWildcardHost(resolvedHost)) {
                return InetAddress.getLoopbackAddress().getHostAddress();
            }
            return resolvedHost;
        }
        if (Objects.equals(advertiseHost, bindHost)) {
            return resolvedHost != null ? resolvedHost : advertiseHost;
        }
        return advertiseHost;
    }

    private InetSocketAddress socketAddress(String host, int port)
    {
        if (host == null || host.isBlank()) {
            return new InetSocketAddress(anyIpv4(), port);
        }
        return new InetSocketAddress(host.trim(), port);
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

    private String normalizeHost(String host)
    {
        if (host == null) {
            return null;
        }
        String trimmed = host.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private Iterable<SocketCandidate> candidateSocketAddresses(String host, int port)
    {
        String normalized = normalizeHost(host);
        var addresses = new java.util.ArrayList<SocketCandidate>(2);
        if (normalized == null || isWildcardHost(normalized)) {
            addresses.add(new SocketCandidate(new InetSocketAddress(anyIpv4(), port), true));
            InetAddress ipv6 = anyIpv6();
            if (ipv6 != null) {
                addresses.add(new SocketCandidate(new InetSocketAddress(ipv6, port), false));
            }
        }
        else {
            addresses.add(new SocketCandidate(new InetSocketAddress(normalized, port), true));
        }
        return addresses;
    }

    private InetAddress anyIpv4()
    {
        try {
            return InetAddress.getByName("0.0.0.0");
        } catch (UnknownHostException e) {
            throw new IllegalStateException("Failed to resolve wildcard IPv4 address", e);
        }
    }

    private InetAddress anyIpv6()
    {
        try {
            return InetAddress.getByName("::");
        } catch (UnknownHostException e) {
            return null;
        }
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

    private record SocketCandidate(InetSocketAddress address, boolean required)
    {
    }
}
