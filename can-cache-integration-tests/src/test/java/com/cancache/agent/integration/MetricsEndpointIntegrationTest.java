package com.cancache.agent.integration;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsEndpointIntegrationTest
{
    @Test
    void prometheusEndpointExposesClusterLabels() throws Exception
    {
        String host = System.getenv("CAN_CACHE_HOST");
        Assumptions.assumeTrue(host != null && !host.isBlank(), "CAN_CACHE_HOST must be provided by the integration environment");

        int port = parseInt(System.getenv("CAN_CACHE_METRICS_PORT"), 9000);
        String path = normalisePath(System.getenv("CAN_CACHE_METRICS_PATH"));

        URI metricsUri = buildUri(host, port, path);
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        String body = fetchMetrics(client, metricsUri);

        assertTrue(body.contains("# TYPE cache_hits_total counter"), "cache_hits_total counter must be exported");
        assertTrue(body.contains("node_id=\""), "node_id label should be present");
        assertTrue(body.contains("role=\""), "role label should be present");
        assertTrue(body.contains("hint_replay_result=\"success\""), "successful hint replay label should be exported");
        assertTrue(body.contains("hint_replay_result=\"failure\""), "failed hint replay label should be exported");
    }

    private static String fetchMetrics(HttpClient client, URI uri) throws IOException, InterruptedException
    {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(2))
                .GET()
                .build();

        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        IOException lastException = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200 && response.body() != null && !response.body().isBlank()) {
                    return response.body();
                }
                lastException = new IOException("Unexpected status code: " + response.statusCode());
            }
            catch (IOException e) {
                lastException = e;
            }
            Thread.sleep(500);
        }

        IOException failure = lastException != null ? lastException : new IOException("No response body");
        throw new IOException("Failed to fetch metrics from " + uri, failure);
    }

    private static int parseInt(String value, int fallback)
    {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        }
        catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static String normalisePath(String path)
    {
        if (path == null || path.isBlank()) {
            return "/metrics";
        }
        String trimmed = path.trim();
        if (!trimmed.startsWith("/")) {
            trimmed = '/' + trimmed;
        }
        return trimmed;
    }

    private static URI buildUri(String host, int port, String path) throws URISyntaxException
    {
        return new URI("http", null, host, port, path, null, null);
    }
}
