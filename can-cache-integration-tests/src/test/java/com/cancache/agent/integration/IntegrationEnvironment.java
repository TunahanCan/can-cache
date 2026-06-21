package com.cancache.agent.integration;

import org.junit.jupiter.api.Assumptions;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

final class IntegrationEnvironment
{
    private static final Duration STARTUP_TIMEOUT =
            Duration.ofSeconds(parseInt(System.getenv("CAN_CACHE_WAIT_TIMEOUT_SECONDS"), 30));
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);

    private IntegrationEnvironment()
    {
    }

    static CacheEndpoint requireCacheEndpoint()
    {
        String host = System.getenv("CAN_CACHE_HOST");
        Assumptions.assumeTrue(host != null && !host.isBlank(),
                "CAN_CACHE_HOST must be provided by the integration environment");

        return new CacheEndpoint(host.trim(), parseInt(System.getenv("CAN_CACHE_PORT"), 11211));
    }

    static MetricsEndpoint requireMetricsEndpoint()
    {
        CacheEndpoint cacheEndpoint = requireCacheEndpoint();
        String host = Optional.ofNullable(System.getenv("CAN_CACHE_METRICS_HOST"))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .orElse(cacheEndpoint.host());
        int port = parseInt(System.getenv("CAN_CACHE_METRICS_PORT"), 9000);
        String path = normalisePath(System.getenv("CAN_CACHE_METRICS_PATH"));
        return new MetricsEndpoint(buildUri(host, port, path));
    }

    static CacheEndpoint requireMetricsCacheEndpoint()
    {
        CacheEndpoint cacheEndpoint = requireCacheEndpoint();
        String host = Optional.ofNullable(System.getenv("CAN_CACHE_METRICS_CACHE_HOST"))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .orElseGet(() -> Optional.ofNullable(System.getenv("CAN_CACHE_METRICS_HOST"))
                        .map(String::trim)
                        .filter(s -> !s.isBlank())
                        .orElse(cacheEndpoint.host()));
        int port = parseInt(System.getenv("CAN_CACHE_METRICS_CACHE_PORT"), cacheEndpoint.port());
        return new CacheEndpoint(host, port);
    }

    static AgentEndpoint requireAgentEndpoint()
    {
        CacheEndpoint cacheEndpoint = requireCacheEndpoint();
        String host = Optional.ofNullable(System.getenv("CAN_CACHE_AGENT_HOST"))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .orElse(cacheEndpoint.host());
        int port = parseInt(System.getenv("CAN_CACHE_AGENT_HTTP_PORT"), 8080);
        String path = normalisePath(Optional.ofNullable(System.getenv("CAN_CACHE_AGENT_STATUS_PATH"))
                .orElse("/agent/instances"));
        return new AgentEndpoint(buildUri(host, port, path));
    }

    static CanCacheClient connect(CacheEndpoint endpoint) throws IOException
    {
        return CanCacheClient.connect(endpoint.host(), endpoint.port(), CONNECT_TIMEOUT);
    }

    static void awaitCacheReady(CacheEndpoint endpoint) throws IOException, InterruptedException
    {
        IOException lastError = null;
        Instant deadline = Instant.now().plus(STARTUP_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            try (CanCacheClient client = connect(endpoint)) {
                String version = client.version();
                if (version.startsWith("VERSION ")) {
                    return;
                }
                lastError = new IOException("Unexpected version response: " + version);
            }
            catch (IOException error) {
                lastError = error;
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }

        throw new IOException("Cache endpoint did not become ready at " + endpoint, lastError);
    }

    private static int parseInt(String value, int fallback)
    {
        return Optional.ofNullable(value)
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .map(raw -> {
                    try {
                        return Integer.parseInt(raw);
                    }
                    catch (NumberFormatException ignored) {
                        return fallback;
                    }
                })
                .orElse(fallback);
    }

    private static String normalisePath(String path)
    {
        if (path == null || path.isBlank()) {
            return "/metrics";
        }
        String trimmed = path.trim();
        return trimmed.startsWith("/") ? trimmed : "/" + trimmed;
    }

    private static URI buildUri(String host, int port, String path)
    {
        try {
            return new URI("http", null, host, port, path, null, null);
        }
        catch (URISyntaxException error) {
            throw new IllegalArgumentException("Invalid metrics endpoint: " + host + ':' + port + path, error);
        }
    }

    record CacheEndpoint(String host, int port)
    {
        @Override
        public String toString()
        {
            return host + ':' + port;
        }
    }

    record MetricsEndpoint(URI uri)
    {
    }

    record AgentEndpoint(URI statusUri)
    {
    }
}
