package com.cancache.agent.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CanCacheProtocolIntegrationTest
{
    private static final Duration EVENTUAL_TIMEOUT = Duration.ofSeconds(4);
    private static IntegrationEnvironment.CacheEndpoint endpoint;

    private CanCacheClient client;

    @BeforeAll
    static void waitForCache() throws Exception
    {
        endpoint = IntegrationEnvironment.requireCacheEndpoint();
        IntegrationEnvironment.awaitCacheReady(endpoint);
    }

    @BeforeEach
    void openClient() throws Exception
    {
        client = IntegrationEnvironment.connect(endpoint);
        expect("OK", client.flushAll());
    }

    @AfterEach
    void closeClient() throws Exception
    {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void storageCommandsRoundTripValuesAndCasMetadata() throws Exception
    {
        String key = key("basic");

        expect("STORED", client.set(key, 7, 0, "hello"));

        CanCacheClient.CacheValue value = value(key);
        Map<String, CanCacheClient.CacheValue> getValues = client.getValues(key, key("missing"));
        Map<String, CanCacheClient.CacheValue> getsValues = client.gets(key);

        assertAll(
                () -> assertEquals("hello", value.asString()),
                () -> assertEquals(7, value.flags()),
                () -> assertEquals(1, getValues.size()),
                () -> assertEquals("hello", getValues.get(key).asString()),
                () -> assertEquals(1, getsValues.size()),
                () -> assertNotNull(getsValues.get(key).cas())
        );

        expect("DELETED", client.delete(key));
        assertMissing(key);
        expect("NOT_FOUND", client.delete(key));
    }

    @Test
    void mutationCommandsRespectExistingAndMissingKeys() throws Exception
    {
        String key = key("mutation");

        expect("STORED", client.add(key, 0, 0, "one"));
        expect("NOT_STORED", client.add(key, 0, 0, "two"));
        expect("STORED", client.replace(key, 0, 0, "two"));
        expect("NOT_STORED", client.replace(key("missing"), 0, 0, "value"));
        expect("STORED", client.append(key, "-tail"));
        expect("STORED", client.prepend(key, "head-"));

        assertValue(key, "head-two-tail");
        expect("NOT_STORED", client.append(key("append-missing"), "x"));
        expect("NOT_STORED", client.prepend(key("prepend-missing"), "x"));
    }

    @Test
    void multiGetReturnsOnlyFoundKeysWithFlags() throws Exception
    {
        String first = key("multi-one");
        String second = key("multi-two");
        String missing = key("multi-missing");

        expect("STORED", client.set(first, 123, 0, "value-1"));
        expect("STORED", client.set(second, 321, 0, "value-2"));

        Map<String, CanCacheClient.CacheValue> values = client.getValues(first, second, missing);
        Map<String, CanCacheClient.CacheValue> casValues = client.gets(first, second, missing);

        assertAll(
                () -> assertEquals(List.of(first, second), new ArrayList<>(values.keySet())),
                () -> assertEquals("value-1", values.get(first).asString()),
                () -> assertEquals("value-2", values.get(second).asString()),
                () -> assertEquals(123, values.get(first).flags()),
                () -> assertEquals(321, values.get(second).flags()),
                () -> assertFalse(values.containsKey(missing)),
                () -> assertEquals(2, casValues.size()),
                () -> assertNotNull(casValues.get(first).cas()),
                () -> assertNotNull(casValues.get(second).cas())
        );
    }

    @Test
    void casCommandsRejectStaleTokensAndUpdateWithCurrentToken() throws Exception
    {
        String key = key("cas");

        expect("STORED", client.set(key, 0, 0, "v1"));
        long firstToken = casToken(key);

        expect("EXISTS", client.cas(key, 0, 0, "stale-write", firstToken + 1));
        assertValue(key, "v1");
        assertEquals(firstToken, casToken(key));
        expect("NOT_FOUND", client.cas(key("cas-missing"), 0, 0, "value", firstToken));

        expect("STORED", client.cas(key, 0, 0, "v2", firstToken));
        long secondToken = casToken(key);

        assertAll(
                () -> assertValue(key, "v2"),
                () -> assertNotEquals(firstToken, secondToken),
                () -> expect("EXISTS", client.cas(key, 0, 0, "old-token", firstToken)),
                () -> expect("STORED", client.cas(key, 0, 0, "v3", secondToken)),
                () -> assertValue(key, "v3")
        );
    }

    @Test
    void numericCommandsHandleBoundsAndTypeErrors() throws Exception
    {
        String numeric = key("numeric");
        String text = key("text");

        expect("STORED", client.set(numeric, 0, 0, "42"));
        expect("STORED", client.set(text, 0, 0, "abc"));

        assertAll(
                () -> expect("50", client.incr(numeric, 8)),
                () -> expect("45", client.decr(numeric, 5)),
                () -> expect("0", client.decr(numeric, 100)),
                () -> expect("NOT_FOUND", client.incr(key("numeric-missing"), 1)),
                () -> expect("CLIENT_ERROR cannot increment or decrement non-numeric value", client.incr(text, 1))
        );
    }

    @Test
    void expirationAndTouchCommandsFollowMemcachedTtlSemantics() throws Exception
    {
        String expiring = key("expires");
        String touched = key("touch");

        expect("STORED", client.set(expiring, 0, 1, "short-lived"));
        assertValue(expiring, "short-lived");
        awaitMissing(expiring);

        expect("STORED", client.set(touched, 0, 1, "extend-me"));
        TimeUnit.MILLISECONDS.sleep(500);
        expect("TOUCHED", client.touch(touched, 2));
        TimeUnit.MILLISECONDS.sleep(800);
        assertValue(touched, "extend-me");
        awaitMissing(touched);

        expect("NOT_FOUND", client.touch(key("touch-missing"), 100));
    }

    @Test
    void flushAllSupportsImmediateAndDelayedInvalidation() throws Exception
    {
        String immediate = key("flush-now");
        String delayed = key("flush-later");

        expect("STORED", client.set(immediate, 0, 0, "1"));
        expect("OK", client.flushAll());
        assertMissing(immediate);

        expect("STORED", client.set(delayed, 0, 0, "2"));
        expect("OK", client.flushAll(Duration.ofSeconds(1)));
        assertValue(delayed, "2");
        awaitMissing(delayed);
    }

    @Test
    void statsAndVersionReflectProtocolTraffic() throws Exception
    {
        Map<String, String> before = client.stats();
        String hit = key("stats-hit");

        expect("STORED", client.set(hit, 0, 0, "value"));
        assertValue(hit, "value");
        assertMissing(key("stats-miss"));

        Map<String, String> after = client.stats();

        assertAll(
                () -> assertEquals(1L, statDelta(before, after, "cmd_set")),
                () -> assertEquals(2L, statDelta(before, after, "cmd_get")),
                () -> assertEquals(1L, statDelta(before, after, "get_hits")),
                () -> assertEquals(1L, statDelta(before, after, "get_misses")),
                () -> assertTrue(stat(after, "curr_items") >= 1),
                () -> assertTrue(client.version().startsWith("VERSION "))
        );
    }

    @Test
    void payloadCommandsPreserveStructuredTextAndBinaryBytes() throws Exception
    {
        List<ExampleDto> originalDtos = List.of(
                new ExampleDto(101, "first", true),
                new ExampleDto(102, "second", false),
                new ExampleDto(103, "third", true)
        );
        String jsonKey = key("dto-list");
        String binaryKey = key("binary");
        byte[] binaryPayload = binaryPayload();

        expect("STORED", client.set(jsonKey, 0, 0, toJson(originalDtos)));
        expect("STORED", client.set(binaryKey, 0, 0, binaryPayload));

        String cachedJson = value(jsonKey).asString();
        CanCacheClient.CacheValue cachedBinary = value(binaryKey);

        assertAll(
                () -> assertEquals(originalDtos, fromJson(cachedJson)),
                () -> assertEquals(0, cachedBinary.flags()),
                () -> assertArrayEquals(binaryPayload, cachedBinary.data())
        );
    }

    @Test
    void payloadCommandsPreserveEmptyAndLargeValues() throws Exception
    {
        String emptyKey = key("empty");
        String largeKey = key("large");
        String largePayload = "payload-".repeat(2048);

        expect("STORED", client.set(emptyKey, 11, 0, ""));
        expect("STORED", client.set(largeKey, 22, 0, largePayload));

        CanCacheClient.CacheValue emptyValue = value(emptyKey);
        CanCacheClient.CacheValue largeValue = value(largeKey);

        assertAll(
                () -> assertEquals(11, emptyValue.flags()),
                () -> assertArrayEquals(new byte[0], emptyValue.data()),
                () -> assertEquals(22, largeValue.flags()),
                () -> assertEquals(largePayload, largeValue.asString())
        );
    }

    @Test
    void protocolErrorsDoNotPoisonConnection() throws Exception
    {
        expect("ERROR", client.command("definitely_unknown_command"));
        expect("CLIENT_ERROR bad command line format", client.command("set incomplete"));

        String key = key("after-error");
        expect("STORED", client.set(key, 0, 0, "still-works"));

        assertAll(
                () -> assertValue(key, "still-works"),
                () -> assertTrue(client.version().startsWith("VERSION "))
        );
    }

    private static String key(String suffix)
    {
        return "it:" + suffix;
    }

    private static byte[] binaryPayload()
    {
        byte[] payload = new byte[256];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }
        return payload;
    }

    private CanCacheClient.CacheValue value(String key) throws IOException
    {
        return client.getValue(key).orElseThrow(() -> new AssertionError("Expected cache value for key " + key));
    }

    private void assertValue(String key, String expected) throws IOException
    {
        assertEquals(expected, value(key).asString(), "Unexpected cache value for " + key);
    }

    private void assertMissing(String key) throws IOException
    {
        assertTrue(client.getValue(key).isEmpty(), "Expected missing cache key " + key);
    }

    private long casToken(String key) throws IOException
    {
        Long cas = client.gets(key).get(key).cas();
        assertNotNull(cas, "Expected CAS token for " + key);
        return cas;
    }

    private void awaitMissing(String key) throws Exception
    {
        Instant deadline = Instant.now().plus(EVENTUAL_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            if (client.getValue(key).isEmpty()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        assertMissing(key);
    }

    private static void expect(String expected, String actual)
    {
        assertEquals(expected, actual);
    }

    private static long statDelta(Map<String, String> before, Map<String, String> after, String key)
    {
        return stat(after, key) - stat(before, key);
    }

    private static long stat(Map<String, String> stats, String key)
    {
        return Long.parseLong(stats.getOrDefault(key, "0"));
    }

    private static String toJson(List<ExampleDto> dtos)
    {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < dtos.size(); i++) {
            ExampleDto dto = dtos.get(i);
            builder.append('{')
                    .append("\"id\":").append(dto.id())
                    .append(",\"name\":\"").append(dto.name()).append('"')
                    .append(",\"active\":").append(dto.active())
                    .append('}');
            if (i < dtos.size() - 1) {
                builder.append(',');
            }
        }
        return builder.append(']').toString();
    }

    private static List<ExampleDto> fromJson(String json)
    {
        String trimmed = json.trim();
        if ("[]".equals(trimmed)) {
            return List.of();
        }

        return Arrays.stream(trimmed.substring(1, trimmed.length() - 1).split("\\},\\{"))
                .map(CanCacheProtocolIntegrationTest::fromJsonObject)
                .toList();
    }

    private static ExampleDto fromJsonObject(String raw)
    {
        String normalized = raw.replace("{", "").replace("}", "");
        int id = 0;
        String name = "";
        boolean active = false;

        for (String field : normalized.split(",")) {
            String[] keyValue = field.split(":", 2);
            String key = keyValue[0].replace("\"", "").trim();
            String value = keyValue[1].replace("\"", "").trim();
            switch (key) {
                case "id" -> id = Integer.parseInt(value);
                case "name" -> name = value;
                case "active" -> active = Boolean.parseBoolean(value);
                default -> throw new IllegalStateException("Unexpected JSON key: " + key);
            }
        }

        return new ExampleDto(id, name, active);
    }

    private record ExampleDto(int id, String name, boolean active)
    {
    }
}
