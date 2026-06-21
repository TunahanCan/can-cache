package com.cancache.agent.integration;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationRepairIntegrationTest
{
    private static final Duration EVENTUAL_TIMEOUT = Duration.ofSeconds(30);
    private static IntegrationEnvironment.CacheEndpoint app1Cache;
    private static IntegrationEnvironment.CacheEndpoint app2Cache;
    private static ReplicationTestClient.Endpoint app1Replication;
    private static ReplicationTestClient.Endpoint app2Replication;

    @BeforeAll
    static void waitForDirectAppsAndCluster() throws Exception
    {
        Assumptions.assumeTrue(isTwoNodeTopology(),
                "Replication repair integration test targets the two-node Docker topology");
        Assumptions.assumeTrue(System.getenv("CAN_CACHE_APP1_HOST") != null
                        && System.getenv("CAN_CACHE_APP2_HOST") != null,
                "Direct app endpoints must be provided by the Docker integration environment");
        app1Cache = cacheEndpoint("CAN_CACHE_APP1_HOST", "CAN_CACHE_APP1_PORT", "can-cache-app-1", 11212);
        app2Cache = cacheEndpoint("CAN_CACHE_APP2_HOST", "CAN_CACHE_APP2_PORT", "can-cache-app-2", 11212);
        app1Replication = ReplicationTestClient.endpoint("CAN_CACHE_APP1_HOST", "CAN_CACHE_APP1_REPLICATION_PORT",
                app1Cache.host(), 18080);
        app2Replication = ReplicationTestClient.endpoint("CAN_CACHE_APP2_HOST", "CAN_CACHE_APP2_REPLICATION_PORT",
                app2Cache.host(), 18080);

        IntegrationEnvironment.awaitCacheReady(app1Cache);
        IntegrationEnvironment.awaitCacheReady(app2Cache);
        awaitClusterCanReadApp1LocalValue();
    }

    @Test
    void readRepairCopiesLocalOnlyValueToMissingReplica() throws Exception
    {
        String key = "repair:read:" + System.nanoTime();
        String encoded = storedValue("read-repaired-value", 4, 101L, 0L);

        seedApp1Local(key, encoded);
        awaitCanCacheValue(app2Cache, key, "read-repaired-value");
        awaitLocalEncodedValue(app2Replication, key, encoded);
    }

    @Test
    void antiEntropyCopiesLocalOnlyValueWithoutClientRead() throws Exception
    {
        String key = "repair:entropy:" + System.nanoTime();
        String encoded = storedValue("entropy-repaired-value", 5, 202L, 0L);

        seedApp1Local(key, encoded);
        awaitLocalEncodedValue(app2Replication, key, encoded);
        awaitCanCacheValue(app2Cache, key, "entropy-repaired-value");
    }

    private static void awaitClusterCanReadApp1LocalValue() throws Exception
    {
        String key = "repair:warmup:" + System.nanoTime();
        String encoded = storedValue("warmup-value", 0, 1L, 0L);
        seedApp1Local(key, encoded);
        awaitCanCacheValue(app2Cache, key, "warmup-value");
    }

    private static void seedApp1Local(String key, String encoded) throws IOException
    {
        try (ReplicationTestClient client = ReplicationTestClient.connect(app1Replication.host(), app1Replication.port())) {
            assertTrue(client.set(key, encoded, 0L));
        }
    }

    private static void awaitCanCacheValue(IntegrationEnvironment.CacheEndpoint endpoint, String key, String expected)
            throws Exception
    {
        Throwable lastError = null;
        Instant deadline = Instant.now().plus(EVENTUAL_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            try (CanCacheClient client = IntegrationEnvironment.connect(endpoint)) {
                Optional<CanCacheClient.CacheValue> value = client.getValue(key);
                if (value.isPresent() && expected.equals(value.get().asString())) {
                    return;
                }
            } catch (Throwable error) {
                lastError = error;
            }
            Thread.sleep(250);
        }
        throw new IOException("Timed out waiting for can-cache value " + key + " at " + endpoint, lastError);
    }

    private static void awaitLocalEncodedValue(ReplicationTestClient.Endpoint endpoint, String key, String expected)
            throws Exception
    {
        Throwable lastError = null;
        Instant deadline = Instant.now().plus(EVENTUAL_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            try (ReplicationTestClient client = ReplicationTestClient.connect(endpoint.host(), endpoint.port())) {
                Optional<String> value = client.get(key);
                if (value.isPresent() && expected.equals(value.get())) {
                    return;
                }
            } catch (Throwable error) {
                lastError = error;
            }
            Thread.sleep(250);
        }
        throw new IOException("Timed out waiting for local replication value " + key + " at " + endpoint, lastError);
    }

    private static IntegrationEnvironment.CacheEndpoint cacheEndpoint(String hostEnv,
                                                                      String portEnv,
                                                                      String fallbackHost,
                                                                      int fallbackPort)
    {
        String host = Optional.ofNullable(System.getenv(hostEnv))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .orElse(fallbackHost);
        int port = Optional.ofNullable(System.getenv(portEnv))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .map(Integer::parseInt)
                .orElse(fallbackPort);
        return new IntegrationEnvironment.CacheEndpoint(host, port);
    }

    private static String storedValue(String value, int flags, long cas, long expireAt) throws IOException
    {
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        ByteBuffer header = ByteBuffer.allocate(20).order(ByteOrder.BIG_ENDIAN);
        header.putLong(cas);
        header.putInt(flags);
        header.putLong(expireAt);

        ByteArrayOutputStream raw = new ByteArrayOutputStream(20 + valueBytes.length);
        try (DataOutputStream output = new DataOutputStream(raw)) {
            output.write(header.array());
            output.write(valueBytes);
        }
        return Base64.getEncoder().encodeToString(raw.toByteArray());
    }

    private static boolean isTwoNodeTopology()
    {
        String rawAppCount = System.getenv("CAN_CACHE_APP_COUNT");
        return rawAppCount == null || rawAppCount.isBlank() || Integer.parseInt(rawAppCount.trim()) == 2;
    }
}
