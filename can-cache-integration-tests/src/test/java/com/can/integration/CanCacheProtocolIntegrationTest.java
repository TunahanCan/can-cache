package com.can.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CanCacheProtocolIntegrationTest
{
    private CanCacheClient client;

    @BeforeEach
    void setUp() throws IOException
    {
        client = CanCacheClient.connectDefault();
        client.flushAll();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void shouldStoreRetrieveAndDeleteValues() throws Exception
    {
        String key = "basic:key";
        assertEquals("STORED", client.set(key, 0, 0, "hello"));
        Optional<CanCacheClient.CacheValue> stored = client.getValue(key);
        assertTrue(stored.isPresent(), "Value must be retrievable after set");
        assertEquals("hello", stored.get().asString());

        Map<String, CanCacheClient.CacheValue> multi = client.getValues(key, "missing");
        assertEquals(1, multi.size());
        assertEquals("hello", multi.get(key).asString());

        Map<String, CanCacheClient.CacheValue> casSnapshot = client.gets(key);
        assertEquals(1, casSnapshot.size());
        Long cas = casSnapshot.get(key).cas();
        assertNotNull(cas, "CAS token must be present");

        assertEquals("DELETED", client.delete(key));
        assertTrue(client.getValue(key).isEmpty(), "Value should be gone after delete");
    }

    @Test
    void shouldHandleAddReplaceAppendAndPrepend() throws Exception
    {
        assertEquals("STORED", client.add("mut", 0, 0, "one"));
        assertEquals("NOT_STORED", client.add("mut", 0, 0, "two"));

        assertEquals("STORED", client.replace("mut", 0, 0, "two"));
        assertEquals("NOT_STORED", client.replace("missing", 0, 0, "value"));

        assertEquals("STORED", client.append("mut", "-app"));
        assertEquals("STORED", client.prepend("mut", "pre-"));

        Optional<CanCacheClient.CacheValue> combined = client.getValue("mut");
        assertTrue(combined.isPresent());
        assertEquals("pre-two-app", combined.get().asString());

        assertEquals("NOT_STORED", client.append("missing", "x"));
        assertEquals("NOT_STORED", client.prepend("missing", "x"));
    }

    @Test
    void shouldSupportCasOperations() throws Exception
    {
        String key = "cas:key";
        assertEquals("STORED", client.set(key, 0, 0, "v1"));
        long cas = client.gets(key).get(key).cas();

        assertEquals("EXISTS", client.cas(key, 0, 0, "v2", cas + 1));
        assertEquals("NOT_FOUND", client.cas("cas:missing", 0, 0, "v", cas));

        assertEquals("STORED", client.cas(key, 0, 0, "v2", cas));
        assertEquals("v2", client.getValue(key).orElseThrow().asString());
    }

    @Test
    void shouldSupportNumericOperations() throws Exception
    {
        String key = "num:key";
        assertEquals("STORED", client.set(key, 0, 0, "42"));
        assertEquals("STORED", client.set("num:string", 0, 0, "abc"));
        assertEquals("50", client.incr(key, 8));
        assertEquals("45", client.decr(key, 5));
        assertEquals("0", client.decr(key, 100));

        assertEquals("NOT_FOUND", client.incr("num:missing", 1));
        assertEquals("CLIENT_ERROR cannot increment or decrement non-numeric value", client.incr("num:string", 1));
    }

    @Test
    void shouldRefreshExpirationWithTouch() throws Exception
    {
        String key = "touch:key";
        assertEquals("STORED", client.set(key, 0, 1, "temp"));

        TimeUnit.MILLISECONDS.sleep(600);
        assertEquals("TOUCHED", client.touch(key, 2));

        TimeUnit.MILLISECONDS.sleep(1100);
        assertTrue(client.getValue(key).isPresent(), "Value should still exist after touch");

        TimeUnit.MILLISECONDS.sleep(1500);
        assertTrue(client.getValue(key).isEmpty(), "Value should expire after extended TTL");
    }

    @Test
    void shouldFlushAllImmediatelyAndWithDelay() throws Exception
    {
        assertEquals("STORED", client.set("flush:one", 0, 0, "1"));
        assertEquals("STORED", client.set("flush:two", 0, 0, "2"));
        assertEquals("OK", client.flushAll());
        assertTrue(client.getValue("flush:one").isEmpty());

        assertEquals("STORED", client.set("flush:delayed", 0, 0, "v"));
        assertEquals("OK", client.flushAll(Duration.ofSeconds(1)));
        assertTrue(client.getValue("flush:delayed").isPresent(), "Value should remain until delay passes");

        TimeUnit.MILLISECONDS.sleep(1200);
        assertTrue(client.getValue("flush:delayed").isEmpty(), "Value should be gone after delayed flush executes");
    }

    @Test
    void shouldExposeStatsAndVersion() throws Exception
    {
        Map<String, String> before = client.stats();

        assertEquals("STORED", client.set("stats:one", 0, 0, "v1"));
        assertEquals("STORED", client.set("stats:two", 0, 0, "v2"));
        assertTrue(client.getValue("stats:one").isPresent());
        assertTrue(client.getValue("missing").isEmpty());

        Map<String, String> after = client.stats();

        long cmdSetDelta = parseLong(after, "cmd_set") - parseLong(before, "cmd_set");
        long cmdGetDelta = parseLong(after, "cmd_get") - parseLong(before, "cmd_get");
        long hitsDelta = parseLong(after, "get_hits") - parseLong(before, "get_hits");
        long missesDelta = parseLong(after, "get_misses") - parseLong(before, "get_misses");

        assertEquals(2L, cmdSetDelta);
        assertEquals(2L, cmdGetDelta);
        assertEquals(1L, hitsDelta);
        assertEquals(1L, missesDelta);
        assertEquals(2L, parseLong(after, "curr_items"));

        String version = client.version();
        assertTrue(version.startsWith("VERSION "));
    }

    private long parseLong(Map<String, String> stats, String key)
    {
        return Long.parseLong(stats.getOrDefault(key, "0"));
    }
}
