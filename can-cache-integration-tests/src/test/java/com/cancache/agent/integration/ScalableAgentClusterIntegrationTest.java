package com.cancache.agent.integration;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScalableAgentClusterIntegrationTest
{
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(3);
    private static final Pattern TOTAL_INSTANCES = Pattern.compile("\"totalInstances\"\\s*:\\s*(\\d+)");
    private static final Pattern HEALTHY_INSTANCES = Pattern.compile("\"healthyInstances\"\\s*:\\s*(\\d+)");
    private static final Pattern INSTANCE_OBJECT = Pattern.compile("\\{[^{}]*\"address\"\\s*:\\s*\"[^\"]+\"[^{}]*}");
    private static final Pattern STRING_FIELD = Pattern.compile("\"%s\"\\s*:\\s*\"([^\"]*)\"");
    private static final Pattern NUMBER_FIELD = Pattern.compile("\"%s\"\\s*:\\s*(\\d+)");

    private static Duration eventualTimeout;
    private static int appCount;
    private static IntegrationEnvironment.CacheEndpoint agentCacheEndpoint;
    private static IntegrationEnvironment.AgentEndpoint agentEndpoint;
    private static List<IntegrationEnvironment.CacheEndpoint> directApps;

    @BeforeAll
    static void waitForScaledTopology() throws Exception
    {
        String rawAppCount = System.getenv("CAN_CACHE_APP_COUNT");
        Assumptions.assumeTrue(rawAppCount != null && !rawAppCount.isBlank(),
                "CAN_CACHE_APP_COUNT must be provided by the scalable Docker integration environment");
        appCount = Integer.parseInt(rawAppCount.trim());
        Assumptions.assumeTrue(List.of(2, 4, 8, 16).contains(appCount),
                "Scalable integration test expects APP_COUNT to be one of 2, 4, 8, or 16");

        eventualTimeout = Duration.ofSeconds(envInt("CAN_CACHE_WAIT_TIMEOUT_SECONDS", 180));
        agentCacheEndpoint = IntegrationEnvironment.requireCacheEndpoint();
        agentEndpoint = IntegrationEnvironment.requireAgentEndpoint();
        directApps = directAppEndpoints(appCount);

        IntegrationEnvironment.awaitCacheReady(agentCacheEndpoint);
        for (IntegrationEnvironment.CacheEndpoint endpoint : directApps) {
            IntegrationEnvironment.awaitCacheReady(endpoint);
        }
        fetchStatusUntil(ScalableAgentClusterIntegrationTest::hasExpectedHealthyInstances);
        awaitClusterConvergence();
    }

    @Test
    void agentRegistersEveryScaledApplication()
    {
        StatusSnapshot status = parseStatus(fetchStatusUntil(ScalableAgentClusterIntegrationTest::hasExpectedHealthyInstances));

        assertAll(
                () -> assertTrue(status.totalInstances() >= appCount, status.body()),
                () -> assertTrue(status.healthyInstances() >= appCount, status.body()),
                () -> assertEquals(expectedAgentAddresses(), new ArrayList<>(status.instances().keySet()))
        );
    }

    @Test
    void agentWritesAndDirectAppWritesAreVisibleFromEveryApplicationDeterministically() throws Exception
    {
        StatusSnapshot before = parseStatus(fetchStatusUntil(ScalableAgentClusterIntegrationTest::hasExpectedHealthyInstances));
        Map<String, String> expectedValues = new LinkedHashMap<>();
        String runId = "scale-" + appCount + '-' + System.nanoTime();
        int writesPerAppThroughAgent = 4;

        for (int i = 0; i < appCount * writesPerAppThroughAgent; i++) {
            String key = runId + ":agent:" + i;
            String value = "agent-value-" + i;
            try (CanCacheClient client = IntegrationEnvironment.connect(agentCacheEndpoint)) {
                assertEquals("STORED", client.set(key, i, 0, value));
            }
            expectedValues.put(key, value);
        }

        StatusSnapshot afterAgentWrites = awaitAgentTrafficDistributed(
                before,
                appCount * writesPerAppThroughAgent
        );

        for (int i = 0; i < directApps.size(); i++) {
            String key = runId + ":direct:" + i;
            String value = "direct-value-" + i;
            try (CanCacheClient client = IntegrationEnvironment.connect(directApps.get(i))) {
                assertEquals("STORED", client.set(key, 100 + i, 0, value));
            }
            expectedValues.put(key, value);
        }

        awaitEveryApplicationSees(expectedValues);
        assertDeterministicReads(expectedValues, 3);

        assertAll(
                () -> assertEquals(appCount, afterAgentWrites.instances().size(), afterAgentWrites.body()),
                () -> assertAgentTrafficReachedEveryApp(before, afterAgentWrites),
                () -> assertTrue(totalAgentConnectionDelta(before, afterAgentWrites) >= appCount * writesPerAppThroughAgent,
                        afterAgentWrites.body()),
                () -> assertFalse(expectedValues.isEmpty()),
                () -> assertEquals(appCount * (writesPerAppThroughAgent + 1), expectedValues.size())
        );
    }

    private static StatusSnapshot awaitAgentTrafficDistributed(StatusSnapshot before, int expectedTotalConnectionDelta)
    {
        return parseStatus(fetchStatusUntil(body -> {
            StatusSnapshot current = parseStatus(body);
            if (!hasExpectedHealthyInstances(body) || current.instances().size() < appCount) {
                return false;
            }
            if (totalAgentConnectionDelta(before, current) < expectedTotalConnectionDelta) {
                return false;
            }
            for (String address : expectedAddresses()) {
                InstanceSnapshot previous = before.instances().get(address);
                InstanceSnapshot next = current.instances().get(address);
                if (previous == null || next == null) {
                    return false;
                }
                if (next.totalConnections() <= previous.totalConnections()) {
                    return false;
                }
                if (next.bytesIn() <= previous.bytesIn() || next.bytesOut() <= previous.bytesOut()) {
                    return false;
                }
            }
            return true;
        }));
    }

    private static void awaitEveryApplicationSees(Map<String, String> expectedValues) throws Exception
    {
        Throwable lastError = null;
        Instant deadline = Instant.now().plus(eventualTimeout);
        while (Instant.now().isBefore(deadline)) {
            try {
                assertEveryApplicationSees(expectedValues);
                return;
            }
            catch (Throwable error) {
                lastError = error;
            }
            Thread.sleep(250);
        }
        throw new IOException("Timed out waiting for every application to see " + expectedValues.size() + " values",
                lastError);
    }

    private static void awaitClusterConvergence() throws Exception
    {
        Throwable lastError = null;
        Instant deadline = Instant.now().plus(eventualTimeout);
        int attempt = 0;
        while (Instant.now().isBefore(deadline)) {
            Map<String, String> probeValues = new LinkedHashMap<>();
            String prefix = "scale-converge-" + appCount + '-' + attempt++ + '-' + System.nanoTime();
            try {
                for (int i = 0; i < directApps.size(); i++) {
                    String key = prefix + ":app:" + i;
                    String value = "converged-value-" + i;
                    try (CanCacheClient client = IntegrationEnvironment.connect(directApps.get(i))) {
                        assertEquals("STORED", client.set(key, i, 30, value));
                    }
                    probeValues.put(key, value);
                }
                assertEveryApplicationSees(probeValues);
                return;
            }
            catch (Throwable error) {
                lastError = error;
            }
            Thread.sleep(500);
        }
        throw new IOException("Timed out waiting for deterministic app-to-app cluster convergence", lastError);
    }

    private static void assertDeterministicReads(Map<String, String> expectedValues, int rounds) throws Exception
    {
        for (int round = 0; round < rounds; round++) {
            assertEveryApplicationSees(expectedValues);
        }
    }

    private static void assertEveryApplicationSees(Map<String, String> expectedValues) throws Exception
    {
        String[] keys = expectedValues.keySet().toArray(String[]::new);
        for (IntegrationEnvironment.CacheEndpoint endpoint : directApps) {
            try (CanCacheClient client = IntegrationEnvironment.connect(endpoint)) {
                Map<String, CanCacheClient.CacheValue> actualValues = client.getValues(keys);
                assertEquals(expectedValues.size(), actualValues.size(), endpoint + " should return every key");
                for (Map.Entry<String, String> expected : expectedValues.entrySet()) {
                    CanCacheClient.CacheValue actual = actualValues.get(expected.getKey());
                    assertTrue(actual != null, endpoint + " should return key " + expected.getKey());
                    assertEquals(expected.getValue(), actual.asString(),
                            endpoint + " returned divergent value for " + expected.getKey());
                }
            }
        }
    }

    private static String fetchStatusUntil(StatusPredicate predicate)
    {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(REQUEST_TIMEOUT)
                .build();
        URI uri = agentEndpoint.statusUri();
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();

        Throwable lastError = null;
        String lastBody = null;
        Instant deadline = Instant.now().plus(eventualTimeout);
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                String body = response.body();
                lastBody = body;
                if (response.statusCode() == 200 && body != null && predicate.matches(body)) {
                    return body;
                }
                lastError = new IOException("Unexpected agent status response status=" + response.statusCode()
                        + " body=" + abbreviate(body));
            }
            catch (Throwable error) {
                lastError = error;
            }
            try {
                Thread.sleep(250);
            }
            catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for agent status", error);
            }
        }

        throw new IllegalStateException("Agent status endpoint did not expose expected scalable topology at " + uri,
                new IOException("lastBody=" + abbreviate(lastBody), lastError));
    }

    private static boolean hasExpectedHealthyInstances(String body)
    {
        StatusSnapshot status = parseStatus(body);
        return status.totalInstances() >= appCount
                && status.healthyInstances() >= appCount
                && status.instances().keySet().containsAll(expectedAddresses());
    }

    private static StatusSnapshot parseStatus(String body)
    {
        Map<String, InstanceSnapshot> instances = new LinkedHashMap<>();
        Matcher matcher = INSTANCE_OBJECT.matcher(body);
        while (matcher.find()) {
            String object = matcher.group();
            String address = stringField(object, "address");
            if (address == null || !address.startsWith("can-cache-app-")) {
                continue;
            }
            instances.put(address, new InstanceSnapshot(
                    address,
                    stringField(object, "state"),
                    longField(object, "totalConnections"),
                    longField(object, "bytesIn"),
                    longField(object, "bytesOut")
            ));
        }
        return new StatusSnapshot(number(body, TOTAL_INSTANCES), number(body, HEALTHY_INSTANCES), instances, body);
    }

    private static List<String> expectedAddresses()
    {
        List<String> addresses = new ArrayList<>(appCount);
        for (int i = 1; i <= appCount; i++) {
            addresses.add("can-cache-app-" + i + ":11212");
        }
        return addresses;
    }

    private static List<String> expectedAgentAddresses()
    {
        return expectedAddresses().stream()
                .sorted(Comparator.comparing(ScalableAgentClusterIntegrationTest::addressHost)
                        .thenComparingInt(ScalableAgentClusterIntegrationTest::addressPort))
                .toList();
    }

    private static String addressHost(String address)
    {
        return address.substring(0, address.lastIndexOf(':'));
    }

    private static int addressPort(String address)
    {
        return Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
    }

    private static void assertAgentTrafficReachedEveryApp(StatusSnapshot before, StatusSnapshot after)
    {
        for (String address : expectedAddresses()) {
            InstanceSnapshot previous = before.instances().get(address);
            InstanceSnapshot current = after.instances().get(address);
            assertTrue(current.totalConnections() > previous.totalConnections(),
                    address + " should receive at least one agent proxied connection");
            assertTrue(current.bytesIn() > previous.bytesIn(), address + " should receive agent request bytes");
            assertTrue(current.bytesOut() > previous.bytesOut(), address + " should send agent response bytes");
        }
    }

    private static long totalAgentConnectionDelta(StatusSnapshot before, StatusSnapshot after)
    {
        long total = 0;
        for (String address : expectedAddresses()) {
            InstanceSnapshot previous = before.instances().get(address);
            InstanceSnapshot current = after.instances().get(address);
            if (previous != null && current != null) {
                total += current.totalConnections() - previous.totalConnections();
            }
        }
        return total;
    }

    private static String abbreviate(String value)
    {
        if (value == null) {
            return "<null>";
        }
        String compact = value.replaceAll("\\s+", " ");
        return compact.length() <= 1_000 ? compact : compact.substring(0, 1_000) + "...";
    }

    private static List<IntegrationEnvironment.CacheEndpoint> directAppEndpoints(int count)
    {
        List<IntegrationEnvironment.CacheEndpoint> endpoints = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            int index = i;
            String host = env("CAN_CACHE_APP_" + index + "_HOST")
                    .or(() -> env("CAN_CACHE_APP" + index + "_HOST"))
                    .orElse("can-cache-app-" + index);
            int port = env("CAN_CACHE_APP_" + index + "_PORT")
                    .or(() -> env("CAN_CACHE_APP" + index + "_PORT"))
                    .map(Integer::parseInt)
                    .orElse(11212);
            endpoints.add(new IntegrationEnvironment.CacheEndpoint(host, port));
        }
        return List.copyOf(endpoints);
    }

    private static Optional<String> env(String name)
    {
        return Optional.ofNullable(System.getenv(name))
                .map(String::trim)
                .filter(value -> !value.isBlank());
    }

    private static int envInt(String name, int fallback)
    {
        return env(name).map(Integer::parseInt).orElse(fallback);
    }

    private static int number(String body, Pattern pattern)
    {
        Matcher matcher = pattern.matcher(body);
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : 0;
    }

    private static String stringField(String object, String field)
    {
        Matcher matcher = Pattern.compile(STRING_FIELD.pattern().formatted(field)).matcher(object);
        return matcher.find() ? matcher.group(1) : null;
    }

    private static long longField(String object, String field)
    {
        Matcher matcher = Pattern.compile(NUMBER_FIELD.pattern().formatted(field)).matcher(object);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : 0L;
    }

    @FunctionalInterface
    private interface StatusPredicate
    {
        boolean matches(String body);
    }

    private record StatusSnapshot(int totalInstances,
                                  int healthyInstances,
                                  Map<String, InstanceSnapshot> instances,
                                  String body)
    {
    }

    private record InstanceSnapshot(String address,
                                    String state,
                                    long totalConnections,
                                    long bytesIn,
                                    long bytesOut)
    {
    }
}
