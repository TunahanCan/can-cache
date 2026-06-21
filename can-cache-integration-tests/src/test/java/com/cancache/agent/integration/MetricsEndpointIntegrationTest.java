package com.cancache.agent.integration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsEndpointIntegrationTest
{
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration EVENTUAL_TIMEOUT = Duration.ofSeconds(30);

    private static IntegrationEnvironment.CacheEndpoint cacheEndpoint;
    private static IntegrationEnvironment.CacheEndpoint metricsCacheEndpoint;
    private static IntegrationEnvironment.MetricsEndpoint metricsEndpoint;

    @BeforeAll
    static void waitForTargets() throws Exception
    {
        cacheEndpoint = IntegrationEnvironment.requireCacheEndpoint();
        metricsCacheEndpoint = IntegrationEnvironment.requireMetricsCacheEndpoint();
        metricsEndpoint = IntegrationEnvironment.requireMetricsEndpoint();
        IntegrationEnvironment.awaitCacheReady(cacheEndpoint);
        IntegrationEnvironment.awaitCacheReady(metricsCacheEndpoint);
    }

    @Test
    void prometheusEndpointExposesCacheAndClusterMetrics() throws Exception
    {
        exerciseCacheCounters();

        String body = fetchMetricsUntil(metricsEndpoint.uri(), MetricsEndpointIntegrationTest::containsExpectedMetrics);

        assertAll(
                () -> assertContains(body, "# TYPE cache_misses counter"),
                () -> assertContains(body, "cache_misses_total{"),
                () -> assertContains(body, "# TYPE cache_get_seconds summary"),
                () -> assertContains(body, "# TYPE cache_set_seconds summary"),
                () -> assertContains(body, "node_id=\""),
                () -> assertContains(body, "role=\""),
                () -> assertContains(body, "hinted_handoff_failures_total{"),
                () -> assertContains(body, "cluster_epoch_increments_total{")
        );
    }

    private static void exerciseCacheCounters() throws IOException
    {
        try (CanCacheClient client = IntegrationEnvironment.connect(metricsCacheEndpoint)) {
            client.flushAll();
            client.set("metrics:hit", 0, 0, "value");
            client.getValue("metrics:hit");
            client.getValue("metrics:miss");
        }
    }

    private static String fetchMetricsUntil(URI uri, MetricsPredicate predicate) throws IOException, InterruptedException
    {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(REQUEST_TIMEOUT)
                .build();
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();

        IOException lastError = null;
        Instant deadline = Instant.now().plus(EVENTUAL_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                String body = response.body();
                if (response.statusCode() == 200 && body != null && predicate.matches(body)) {
                    return body;
                }
                lastError = new IOException("Unexpected metrics response status=" + response.statusCode());
            }
            catch (IOException error) {
                lastError = error;
            }
            Thread.sleep(250);
        }

        throw new IOException("Metrics endpoint did not expose expected data at " + uri, lastError);
    }

    private static boolean containsExpectedMetrics(String body)
    {
        return body.contains("# TYPE cache_misses counter")
                && body.contains("cache_misses_total{")
                && body.contains("# TYPE cache_get_seconds summary")
                && body.contains("# TYPE cache_set_seconds summary")
                && body.contains("node_id=\"")
                && body.contains("role=\"")
                && body.contains("hinted_handoff_failures_total{")
                && body.contains("cluster_epoch_increments_total{");
    }

    private static void assertContains(String body, String expected)
    {
        assertTrue(body.contains(expected), "Metrics body should contain: " + expected);
    }

    @FunctionalInterface
    private interface MetricsPredicate
    {
        boolean matches(String body);
    }
}
