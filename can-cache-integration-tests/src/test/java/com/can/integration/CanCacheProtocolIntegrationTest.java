package com.can.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CanCacheProtocolIntegrationTest
{
    private CanCacheClient client;

    @BeforeAll
    static void requireConfiguredTarget()
    {
        Assumptions.assumeTrue(System.getenv("CAN_CACHE_HOST") != null,
                "CAN_CACHE_HOST must be provided by the integration environment");
    }

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
    void shouldRetrieveMultipleKeysWithConsistentMetadata() throws Exception
    {
        // Senaryo: Çoklu get isteğinde birden fazla anahtarın aynı anda okunurken bayrak ve değer bilgilerinin doğru döndüğünü doğruluyoruz.
        assertEquals("STORED", client.set("multi:one", 123, 0, "deger-1"));
        assertEquals("STORED", client.set("multi:two", 321, 0, "deger-2"));

        Map<String, CanCacheClient.CacheValue> values = client.getValues("multi:one", "multi:two", "multi:missing");
        assertEquals(2, values.size());
        assertTrue(values.containsKey("multi:one"));
        assertTrue(values.containsKey("multi:two"));
        assertEquals("deger-1", values.get("multi:one").asString());
        assertEquals("deger-2", values.get("multi:two").asString());
        assertEquals(123, values.get("multi:one").flags());
        assertEquals(321, values.get("multi:two").flags());

        Map<String, CanCacheClient.CacheValue> casValues = client.gets("multi:one", "multi:two");
        assertEquals(2, casValues.size());
        assertNotNull(casValues.get("multi:one").cas());
        assertNotNull(casValues.get("multi:two").cas());
    }

    @Test
    void shouldSupportCasOperations() throws Exception
    {
        String key = "cas:key";
        assertEquals("STORED", client.set(key, 0, 0, "v1"));
        long originalCas = client.gets(key).get(key).cas();

        assertEquals("EXISTS", client.cas(key, 0, 0, "v2", originalCas + 1));
        assertEquals("v1", client.getValue(key).orElseThrow().asString(),
                "Başarısız CAS denemesi değeri değiştirmemeli");

        long currentCas = client.gets(key).get(key).cas();
        assertEquals(originalCas, currentCas, "Başarısız CAS yeni bir token üretmemeli");
        assertEquals("NOT_FOUND", client.cas("cas:missing", 0, 0, "v", currentCas));

        assertEquals("STORED", client.cas(key, 0, 0, "v2", currentCas));
        assertEquals("v2", client.getValue(key).orElseThrow().asString());
    }

    @Test
    void shouldUpdateCasTokensAfterMutation() throws Exception
    {
        // Senaryo: Aynı anahtar üzerinde güncelleme yapıldığında CAS bilgisinin değiştiğini ve eski token ile güncellemenin reddedildiğini kontrol ediyoruz.
        String key = "cas:tracking";
        assertEquals("STORED", client.set(key, 0, 0, "once"));

        long firstCas = client.gets(key).get(key).cas();
        assertNotNull(firstCas);

        assertEquals("STORED", client.set(key, 0, 0, "iki"));

        long secondCas = client.gets(key).get(key).cas();
        assertNotNull(secondCas);
        assertNotEquals(firstCas, secondCas);

        assertEquals("EXISTS", client.cas(key, 0, 0, "uc", firstCas));
        assertEquals("STORED", client.cas(key, 0, 0, "uc", secondCas));
        assertEquals("uc", client.getValue(key).orElseThrow().asString());
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
    void shouldCacheArrayListOfDtoPayloads() throws Exception
    {
        // Senaryo: Örnek DTO nesnelerinden oluşan bir ArrayList'i JSON'a çevirerek saklayıp tekrar okuduğumuzda veri bütünlüğü
        // korunuyor mu kontrol ediyoruz.
        List<ExampleDto> originalDtos = new ArrayList<>();
        originalDtos.add(new ExampleDto(101, "ilk", true));
        originalDtos.add(new ExampleDto(102, "ikinci", false));
        originalDtos.add(new ExampleDto(103, "ucuncu", true));

        String jsonPayload = toJson(originalDtos);
        String key = "dto:list";

        assertEquals("STORED", client.set(key, 0, 0, jsonPayload));

        String cachedJson = client.getValue(key).orElseThrow().asString();
        List<ExampleDto> cachedDtos = fromJson(cachedJson);

        assertEquals(jsonPayload, cachedJson, "Serileştirilmiş JSON metni cache tarafından eksiksiz korunmalıdır");
        assertEquals(originalDtos, cachedDtos, "Cache'ten dönen DTO listesi orijinal listeyle birebir aynı olmalıdır");
    }

    @Test
    void shouldRespectExpirationTimes() throws Exception
    {
        // Senaryo: Kısa yaşam süresi ile yazılan bir değerin süre sonunda otomatik olarak silindiğini gözlemliyoruz.
        String key = "expire:key";
        assertEquals("STORED", client.set(key, 0, 1, "gecici"));
        assertTrue(client.getValue(key).isPresent());

        TimeUnit.MILLISECONDS.sleep(1200);
        assertTrue(client.getValue(key).isEmpty(), "Değer süre sonunda otomatik düşmelidir");
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
    void shouldHandleTouchForMissingKeys() throws Exception
    {
        // Senaryo: Olmayan bir anahtar üzerinde touch çağrısı yapıldığında NOT_FOUND yanıtının döndüğünü test ediyoruz.
        assertEquals("NOT_FOUND", client.touch("touch:missing", 100));
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

    @Test
    void shouldPreserveBinaryPayloads() throws Exception
    {
        // Senaryo: Binary içerikli verinin saklanıp tekrar okunurken hiçbir byte kaybı yaşanmadığını doğruluyoruz.
        byte[] payload = new byte[256];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }

        String key = "bin:key";
        assertEquals("STORED", client.set(key, 0, 0, new String(payload, StandardCharsets.ISO_8859_1)));

        CanCacheClient.CacheValue value = client.getValue(key).orElseThrow();
        assertEquals(payload.length, value.data().length);
        for (int i = 0; i < payload.length; i++) {
            assertEquals(payload[i], value.data()[i]);
        }
    }

    private String toJson(List<ExampleDto> dtos)
    {
        StringBuilder builder = new StringBuilder();
        builder.append('[');
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
        builder.append(']');
        return builder.toString();
    }

    private List<ExampleDto> fromJson(String json)
    {
        String trimmed = json.trim();
        List<ExampleDto> result = new ArrayList<>();
        if (trimmed.equals("[]")) {
            return result;
        }

        String inner = trimmed.substring(1, trimmed.length() - 1);
        String[] entries = inner.split("\\},\\{");
        for (String entry : entries) {
            String normalized = entry.replace("{", "").replace("}", "");
            String[] fields = normalized.split(",");
            int id = 0;
            String name = "";
            boolean active = false;
            for (String field : fields) {
                String[] keyValue = field.split(":", 2);
                String key = keyValue[0].replace("\"", "").trim();
                String value = keyValue[1].replace("\"", "").trim();
                switch (key) {
                    case "id" -> id = Integer.parseInt(value);
                    case "name" -> name = value;
                    case "active" -> active = Boolean.parseBoolean(value);
                    default -> throw new IllegalStateException("Unexpected key: " + key);
                }
            }
            result.add(new ExampleDto(id, name, active));
        }
        return result;
    }

    private long parseLong(Map<String, String> stats, String key)
    {
        return Long.parseLong(stats.getOrDefault(key, "0"));
    }

    private record ExampleDto(int id, String name, boolean active)
    {
    }
}
