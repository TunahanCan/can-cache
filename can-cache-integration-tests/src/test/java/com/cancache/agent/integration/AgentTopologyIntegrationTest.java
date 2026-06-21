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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgentTopologyIntegrationTest
{
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration EVENTUAL_TIMEOUT = Duration.ofSeconds(30);
    private static final Pattern TOTAL_INSTANCES = Pattern.compile("\"totalInstances\"\\s*:\\s*(\\d+)");
    private static final Pattern HEALTHY_INSTANCES = Pattern.compile("\"healthyInstances\"\\s*:\\s*(\\d+)");

    private static IntegrationEnvironment.CacheEndpoint cacheEndpoint;
    private static IntegrationEnvironment.AgentEndpoint agentEndpoint;

    @BeforeAll
    static void waitForAgentTopology() throws Exception
    {
        cacheEndpoint = IntegrationEnvironment.requireCacheEndpoint();
        agentEndpoint = IntegrationEnvironment.requireAgentEndpoint();
        IntegrationEnvironment.awaitCacheReady(cacheEndpoint);
        fetchStatusUntil(AgentTopologyIntegrationTest::hasTwoHealthyInstances);
    }

    @Test
    void agentRegistersTwoHealthyCacheApplications() throws Exception
    {
        String status = fetchStatusUntil(AgentTopologyIntegrationTest::hasTwoHealthyInstances);

        assertAll(
                () -> assertTrue(number(status, TOTAL_INSTANCES) >= 2, status),
                () -> assertTrue(number(status, HEALTHY_INSTANCES) >= 2, status),
                () -> assertTrue(status.contains("can-cache-app-1:11212"), status),
                () -> assertTrue(status.contains("can-cache-app-2:11212"), status)
        );
    }

    @Test
    void agentRoutesIndependentConnectionsAcrossBothApplications() throws Exception
    {
        for (int i = 0; i < 8; i++) {
            try (CanCacheClient client = IntegrationEnvironment.connect(cacheEndpoint)) {
                assertTrue(client.version().startsWith("VERSION "));
            }
        }

        String status = fetchStatusUntil(body ->
                body.contains("\"upstream\":\"can-cache-app-1:11212\"")
                        && body.contains("\"upstream\":\"can-cache-app-2:11212\""));

        assertAll(
                () -> assertTrue(status.contains("\"recentConnections\""), status),
                () -> assertTrue(status.contains("\"upstream\":\"can-cache-app-1:11212\""), status),
                () -> assertTrue(status.contains("\"upstream\":\"can-cache-app-2:11212\""), status)
        );
    }

    private static String fetchStatusUntil(StatusPredicate predicate) throws IOException, InterruptedException
    {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(REQUEST_TIMEOUT)
                .build();
        URI uri = agentEndpoint.statusUri();
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
                lastError = new IOException("Unexpected agent status response status=" + response.statusCode());
            }
            catch (IOException error) {
                lastError = error;
            }
            Thread.sleep(250);
        }

        throw new IOException("Agent status endpoint did not expose expected topology at " + uri, lastError);
    }

    private static boolean hasTwoHealthyInstances(String body)
    {
        return number(body, TOTAL_INSTANCES) >= 2
                && number(body, HEALTHY_INSTANCES) >= 2
                && body.contains("can-cache-app-1:11212")
                && body.contains("can-cache-app-2:11212");
    }

    private static int number(String body, Pattern pattern)
    {
        Matcher matcher = pattern.matcher(body);
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : 0;
    }

    @FunctionalInterface
    private interface StatusPredicate
    {
        boolean matches(String body);
    }
}
