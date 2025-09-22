package com.can.cache.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class cancachedProtocolTest {

    private MemcacheTextClient client;

    @BeforeAll
    static void waitForService() throws Exception {
        MemcacheTextClient.waitForService(Duration.ofSeconds(30));
    }

    @BeforeEach
    void setUp() throws IOException {
        client = new MemcacheTextClient();
        client.connect();
        client.flushAll();
    }

    @AfterEach
    void tearDown() {
        try {
            client.flushAll();
        } catch (IOException ignored) {
            // Ignore cleanup failures if the server terminated mid-test.
        }
        client.close();
    }

    @Test
    void setAndGetRoundTrip() throws IOException {
        assertEquals("STORED", client.set("alpha", "payload", 7, 0));

        Map<String, MemcacheTextClient.ValueRecord> values = client.get("alpha");
        MemcacheTextClient.ValueRecord record = values.get("alpha");
        assertNotNull(record);
        assertEquals(7, record.flags());
        assertEquals("payload", record.value());
    }

    @Test
    void addReplaceAppendAndPrepend() throws IOException {
        assertEquals("STORED", client.add("greeting", "Hello", 0, 0));
        assertEquals("NOT_STORED", client.add("greeting", "Ignored", 0, 0));
        assertEquals("STORED", client.replace("greeting", "World", 0, 0));
        assertEquals("NOT_STORED", client.replace("missing", "nope", 0, 0));
        assertEquals("STORED", client.append("greeting", "!"));
        assertEquals("STORED", client.prepend("greeting", "Say:"));

        String value = client.get("greeting").get("greeting").value();
        assertEquals("Say:World!", value);
    }

    @Test
    void deleteClearsExistingValues() throws IOException {
        assertEquals("STORED", client.set("tmp", "1", 0, 0));
        assertEquals("DELETED", client.delete("tmp"));
        assertTrue(client.get("tmp").isEmpty());
        assertEquals("NOT_FOUND", client.delete("tmp"));
    }

    @Test
    void casFlowDetectsWriteConflicts() throws IOException {
        assertEquals("STORED", client.set("item", "first", 0, 0));
        MemcacheTextClient.ValueRecord initial = client.gets("item").get("item");
        assertNotNull(initial);
        Long casToken = initial.casToken();
        assertNotNull(casToken);

        assertEquals("STORED", client.cas("item", "second", casToken, 0, 0));

        String staleResult = client.cas("item", "third", casToken, 0, 0);
        assertEquals("EXISTS", staleResult);

        String latest = client.get("item").get("item").value();
        assertEquals("second", latest);
    }

    @Test
    void incrAndDecrFollowcancachedSemantics() throws IOException {
        assertEquals("STORED", client.set("counter", "10", 0, 0));
        assertEquals(15L, client.incr("counter", 5));
        assertEquals(12L, client.decr("counter", 3));
        assertEquals(0L, client.decr("counter", 50));
        assertNull(client.incr("missing", 1));
        assertNull(client.decr("missing", 1));
    }

    @Test
    void touchAndExpiration() throws Exception {
        assertEquals("STORED", client.set("ephemeral", "data", 0, 1));
        Thread.sleep(600);
        assertEquals("TOUCHED", client.touch("ephemeral", 2));
        assertTrue(client.get("ephemeral").containsKey("ephemeral"));
        Thread.sleep(1100);
        assertTrue(client.get("ephemeral").containsKey("ephemeral"));
        Thread.sleep(1200);
        assertTrue(client.get("ephemeral").isEmpty());
        assertEquals("NOT_FOUND", client.touch("missing", 5));
    }

    @Test
    void flushAllInvalidatesEverything() throws IOException {
        assertEquals("STORED", client.set("k1", "v1", 0, 0));
        assertEquals("STORED", client.set("k2", "v2", 0, 0));
        assertEquals("OK", client.flushAll());
        assertTrue(client.get("k1").isEmpty());
        assertTrue(client.get("k2").isEmpty());
    }

    @Test
    void multiGetReturnsAllKeys() throws IOException {
        assertEquals("STORED", client.set("a", "1", 0, 0));
        assertEquals("STORED", client.set("b", "2", 0, 0));
        Map<String, MemcacheTextClient.ValueRecord> results = client.get("a", "b", "missing");
        assertEquals("1", results.get("a").value());
        assertEquals("2", results.get("b").value());
        assertFalse(results.containsKey("missing"));
    }

    @Test
    void statsAndVersionCommands() throws IOException {
        Map<String, String> stats = client.stats();
        assertTrue(stats.containsKey("curr_items"));
        assertTrue(stats.containsKey("cmd_get"));

        String version = client.version();
        assertTrue(version.startsWith("VERSION"));
    }
}
