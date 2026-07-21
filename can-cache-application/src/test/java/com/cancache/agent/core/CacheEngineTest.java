package com.cancache.agent.core;

import com.cancache.agent.codec.StringCodec;
import com.cancache.agent.constants.NodeProtocol;
import com.cancache.agent.metric.MetricsRegistry;
import com.cancache.agent.metric.Timer;
import com.cancache.agent.pubsub.Broker;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class CacheEngineTest
{
    private Vertx vertx;
    private CacheEngine<String, String> engine;
    private MetricsRegistry metrics;
    private RecordingBroker broker;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        metrics = new MetricsRegistry();
        broker = new RecordingBroker();
        engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                .segments(2)
                .maxCapacity(16)
                .cleanerPollMillis(5)
                .metrics(metrics)
                .broker(broker)
                .vertx(vertx)
                .build();
    }

    @AfterEach
    void cleanup()
    {
        if (engine != null)
        {
            engine.close();
        }
        if (broker != null)
        {
            broker.close();
        }
        if (vertx != null)
        {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    @Nested
    class SetAndGetBehavior
    {
        /**
         * Verifies that set and get calls preserve the value and update the metrics accordingly.
         */
        @Test
        void shouldPreserveValueAndUpdateMetricsOnSetAndGet()
        {
            // Given / When
            boolean setSuccess = engine.set("key", "value");

            // Then
            assertTrue(setSuccess, "Set operation should succeed");
            assertEquals("value", engine.get("key"), "Get operation should return the set value");
            assertEquals(1, engine.size(), "Cache size should be 1");

            assertEquals(1L, metrics.counter("cache_hits").get(), "Cache hits should be 1");
            Timer.Sample setSample = metrics.timer("cache_set").snapshot();
            assertTrue(setSample.count() >= 1, "Cache set timer should have at least 1 sample");
            Timer.Sample getSample = metrics.timer("cache_get").snapshot();
            assertTrue(getSample.count() >= 1, "Cache get timer should have at least 1 sample");
            assertTrue(broker.events().contains("keyspace:set:key"), "Broker should emit keyspace:set event");
        }

        @Test
        void shouldRejectNullValues()
        {
            assertThrows(NullPointerException.class, () -> engine.set("key", null));
        }

        /**
         * Demonstrates that the get call removes the entry after its TTL expires.
         */
        @Test
        void shouldRemoveEntryOnGetAfterTtlExpires()
        {
            // Given
            assertTrue(engine.set("expire", "value", Duration.ofMillis(15)), "Set with TTL should succeed");

            // When
            sleep(40);

            // Then
            assertNull(engine.get("expire"), "Get should return null for expired key");
            assertFalse(engine.exists("expire"), "Exists should return false for expired key");
            assertTrue(metrics.counter("cache_misses").get() >= 1, "Cache misses should be incremented");
            assertTrue(broker.events().contains("keyspace:del:expire"), "Broker should emit keyspace:del event");
        }

        /**
         * Checks that extremely large TTL values are stored without causing an overflow.
         */
        @Test
        void shouldStoreExtremeTtlWithoutOverflow()
        {
            // Given
            long now = System.currentTimeMillis();
            Duration ttl = Duration.ofMillis(Long.MAX_VALUE - now - 1);

            // When
            boolean setSuccess = engine.set("forever", "value", ttl);

            // Then
            assertTrue(setSuccess, "Set with extreme TTL should succeed");
            assertEquals("value", engine.get("forever"), "Value should be accessible");
            assertTrue(engine.exists("forever"), "Key should exist");
        }
    }

    @Nested
    class CompareAndSwapBehavior
    {
        /**
         * Proves that the value and TTL are updated when the CAS expectation is met.
         */
        @Test
        void shouldUpdateValueWhenCasMatches()
        {
            // Given
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue("v1".getBytes(StandardCharsets.UTF_8), 1, 9L, 0L);
            String encoded = StoredValueCodec.encode(base);
            assertTrue(engine.set("cas", encoded), "Initial set should succeed");

            StoredValueCodec.StoredValue updated = base.withValue("v2".getBytes(StandardCharsets.UTF_8), 11L);
            String next = StoredValueCodec.encode(updated);

            // When
            boolean casSuccess = engine.compareAndSwap("cas", next, 9L, Duration.ofMillis(30));

            // Then
            assertTrue(casSuccess, "CAS should succeed when CAS value matches");
            assertEquals(next, engine.get("cas"), "Cache should contain the newly updated value");
            
            // Wait for expiration
            sleep(60);
            assertFalse(engine.exists("cas"), "Key should be removed after new TTL expires");
            assertTrue(broker.events().stream().anyMatch(e -> e.startsWith("keyspace:set:cas")), "Broker should record set event");
        }

        /**
         * Verifies that the value remains unchanged when the CAS expectation does not match.
         */
        @Test
        void shouldFailWhenCasMismatchOccurs()
        {
            // Given
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue("v1".getBytes(StandardCharsets.UTF_8), 1, 7L, 0L);
            String encoded = StoredValueCodec.encode(base);
            assertTrue(engine.set("cas", encoded), "Initial set should succeed");

            // When
            boolean casSuccess = engine.compareAndSwap("cas", "ignored", 5L, null);

            // Then
            assertFalse(casSuccess, "CAS should fail when CAS value mismatches");
            assertEquals(encoded, engine.get("cas"), "Original value should be intact");
        }

        /**
         * Verifies that the record is cleared if a CAS attempt is made on an expired entry.
         */
        @Test
        void shouldRemoveExpiredEntryOnCompareAndSwap()
        {
            // Given
            assertTrue(engine.set("stale", "plain", Duration.ofMillis(10)), "Set with TTL should succeed");
            sleep(30);

            // When
            boolean casSuccess = engine.compareAndSwap("stale", "new", 0L, null);

            // Then
            assertFalse(casSuccess, "CAS should fail on expired entry");
            assertFalse(engine.exists("stale"), "Key should be removed after attempting CAS on expired entry");
            assertTrue(broker.events().stream().anyMatch(e -> e.startsWith("keyspace:del:stale")), "Broker should record delete event");
        }

        @Test
        void shouldRemoveTheOldTtlWhenCasUsesNoExpiration()
        {
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue(
                    "v1".getBytes(StandardCharsets.UTF_8), 1, 17L, 0L);
            assertTrue(engine.set("cas:forever", StoredValueCodec.encode(base), Duration.ofMillis(25)));

            StoredValueCodec.StoredValue updated = base.withValue(
                    "v2".getBytes(StandardCharsets.UTF_8), 18L);
            assertTrue(engine.compareAndSwap(
                    "cas:forever", StoredValueCodec.encode(updated), 17L, null));

            sleep(60);
            assertEquals(StoredValueCodec.encode(updated), engine.get("cas:forever"),
                    "A null CAS TTL must replace, rather than preserve, the previous TTL");
        }

        @Test
        void shouldConditionallyRemoveEntryWhenCasExpiresImmediately()
        {
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue(
                    "v1".getBytes(StandardCharsets.UTF_8), 1, 23L, 0L);
            assertTrue(engine.set("cas:expire-now", StoredValueCodec.encode(base)));

            assertTrue(engine.compareAndSwap(
                    "cas:expire-now", StoredValueCodec.encode(base), 23L, Duration.ZERO));
            assertFalse(engine.exists("cas:expire-now"));
        }

        @Test
        void shouldAtomicallyCreateOnlyWhenAbsent()
        {
            StoredValueCodec.StoredValue created = new StoredValueCodec.StoredValue(
                    "created".getBytes(StandardCharsets.UTF_8), 0, 31L, 0L);
            String encoded = StoredValueCodec.encode(created);

            assertTrue(engine.compareAndSwap(
                    "cas:add", encoded, NodeProtocol.CAS_EXPECT_ABSENT, null));
            assertFalse(engine.compareAndSwap(
                    "cas:add", "replacement", NodeProtocol.CAS_EXPECT_ABSENT, null));
            assertEquals(encoded, engine.get("cas:add"));
        }

        @Test
        void shouldTreatAnExpiredEntryAsAbsentForAtomicCreate()
        {
            StoredValueCodec.StoredValue stale = new StoredValueCodec.StoredValue(
                    "stale".getBytes(StandardCharsets.UTF_8), 0, 37L, 0L);
            assertTrue(engine.set("cas:add-expired", StoredValueCodec.encode(stale), Duration.ofMillis(10)));
            sleep(30);

            StoredValueCodec.StoredValue fresh = new StoredValueCodec.StoredValue(
                    "fresh".getBytes(StandardCharsets.UTF_8), 0, 38L, 0L);
            String encodedFresh = StoredValueCodec.encode(fresh);
            assertTrue(engine.compareAndSwap(
                    "cas:add-expired", encodedFresh, NodeProtocol.CAS_EXPECT_ABSENT, null));
            assertEquals(encodedFresh, engine.get("cas:add-expired"));
        }

        @Test
        void shouldAllowExactlyOneConcurrentAtomicCreate() throws Exception
        {
            String first = StoredValueCodec.encode(new StoredValueCodec.StoredValue(
                    "first".getBytes(StandardCharsets.UTF_8), 0, 41L, 0L));
            String second = StoredValueCodec.encode(new StoredValueCodec.StoredValue(
                    "second".getBytes(StandardCharsets.UTF_8), 0, 42L, 0L));
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);

            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<Boolean> firstResult = executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    return engine.compareAndSwap(
                            "cas:add-race", first, NodeProtocol.CAS_EXPECT_ABSENT, null);
                });
                Future<Boolean> secondResult = executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    return engine.compareAndSwap(
                            "cas:add-race", second, NodeProtocol.CAS_EXPECT_ABSENT, null);
                });

                ready.await();
                start.countDown();
                boolean firstWon = firstResult.get();
                boolean secondWon = secondResult.get();

                assertNotEquals(firstWon, secondWon, "Exactly one create-if-absent operation must win");
                assertEquals(firstWon ? first : second, engine.get("cas:add-race"));
            }
        }
    }

    @Nested
    class ResourceLimits
    {
        @Test
        void shouldEvictByEncodedPayloadWeight()
        {
            replaceEngine(CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(10)
                    .maxWeightBytes(5)
                    .cleanerPollMillis(60_000)
                    .vertx(vertx)
                    .build());

            assertTrue(engine.set("a", "123"));
            assertTrue(engine.set("b", "456"));

            assertNull(engine.get("a"));
            assertEquals("456", engine.get("b"));
            assertEquals(3L, engine.estimatedPayloadBytes());
        }

        @Test
        void shouldRejectOversizedReplacementWithoutLosingExistingValue()
        {
            replaceEngine(CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(10)
                    .maxWeightBytes(5)
                    .cleanerPollMillis(60_000)
                    .vertx(vertx)
                    .build());

            assertTrue(engine.set("key", "ok"));
            assertFalse(engine.set("key", "too-big"));

            assertEquals("ok", engine.get("key"));
            assertEquals(2L, engine.estimatedPayloadBytes());
        }

        @Test
        void shouldCompactStaleExpirationTasksForAHotKey()
        {
            replaceEngine(CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(8)
                    .maxWeightBytes(1_024)
                    .cleanerPollMillis(60_000)
                    .vertx(vertx)
                    .build());

            for (int i = 0; i < 1_000; i++) {
                assertTrue(engine.set("hot", "v" + i, Duration.ofHours(1)));
            }

            assertTrue(engine.scheduledExpirationCount() <= 128,
                    "The synchronous safety valve must bound stale TTL tasks even before manual compaction");
            engine.compactExpirationQueue();
            assertEquals(1, engine.scheduledExpirationCount());

            assertTrue(engine.delete("hot"));
            engine.compactExpirationQueue();
            assertEquals(0, engine.scheduledExpirationCount());
        }
    }

    @Nested
    class ReplayBehavior
    {
        /**
         * Shows that a set record originating from a persistent log is restored to memory.
         */
        @Test
        void shouldRestoreValueOnReplaySetCommand()
        {
            // Given / When
            engine.replay(new byte[]{NodeProtocol.CMD_SET}, StringCodec.UTF8.encode("key"), StringCodec.UTF8.encode("value"), 0L);

            // Then
            assertEquals("value", engine.get("key"), "Replayed value should be accessible via get");
        }

        /**
         * Checks that an expired replay entry is ignored.
         */
        @Test
        void shouldIgnoreExpiredRecordOnReplay()
        {
            // Given / When
            engine.replay(new byte[]{NodeProtocol.CMD_SET}, StringCodec.UTF8.encode("late"), StringCodec.UTF8.encode("value"), System.currentTimeMillis() - 1_000);

            // Then
            assertNull(engine.get("late"), "Expired replayed value should not be accessible");
        }

        /**
         * Verifies that a replay delete record removes the corresponding key.
         */
        @Test
        void shouldRemoveEntryOnReplayDeleteCommand()
        {
            // Given
            assertTrue(engine.set("gone", "value"), "Initial set should succeed");

            // When
            engine.replay(new byte[]{NodeProtocol.CMD_DELETE}, StringCodec.UTF8.encode("gone"), new byte[0], 0L);

            // Then
            assertNull(engine.get("gone"), "Replayed delete should remove the value");
        }

        @Test
        void shouldRejectMalformedReplayCommands()
        {
            assertThrows(IllegalArgumentException.class,
                    () -> engine.replay(new byte[0], StringCodec.UTF8.encode("key"), new byte[0], 0L));
            assertThrows(IllegalArgumentException.class,
                    () -> engine.replay(new byte[]{99}, StringCodec.UTF8.encode("key"), new byte[0], 0L));
        }

        @Test
        void shouldNotResurrectAnOlderValueWhenLatestReplayRecordDoesNotFit()
        {
            replaceEngine(CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(10)
                    .maxWeightBytes(5)
                    .cleanerPollMillis(60_000)
                    .vertx(vertx)
                    .build());

            engine.replay(new byte[]{NodeProtocol.CMD_SET}, StringCodec.UTF8.encode("key"),
                    StringCodec.UTF8.encode("old"), 0L);
            engine.replay(new byte[]{NodeProtocol.CMD_SET}, StringCodec.UTF8.encode("key"),
                    StringCodec.UTF8.encode("latest-is-too-large"), 0L);

            assertNull(engine.get("key"));
        }
    }

    @Nested
    class RemovalNotifications
    {
        /**
         * Verifies that the listener is notified when a manual deletion is performed.
         */
        @Test
        void shouldNotifyListenerOnManualDelete() throws Exception
        {
            // Given
            List<String> removed = new ArrayList<>();
            AutoCloseable handle = engine.onRemoval(removed::add);
            assertTrue(engine.set("target", "value"), "Initial set should succeed");

            // When
            assertTrue(engine.delete("target"), "Manual delete should succeed");

            // Then
            assertEquals(List.of("target"), removed, "Listener should receive the removed key");
            long deleteEvents = broker.events().stream()
                    .filter("keyspace:del:target"::equals)
                    .count();
            assertEquals(1L, deleteEvents, "Manual delete should publish exactly one event");
            handle.close();
        }

        /**
         * Proves that a notification is sent to the listener when TTL expires.
         */
        @Test
        void shouldNotifyListenerOnTtlExpiration()
        {
            // Given
            List<String> removed = new ArrayList<>();
            engine.onRemoval(removed::add);
            assertTrue(engine.set("ttl", "value", Duration.ofMillis(15)), "Set with TTL should succeed");

            // When
            sleep(60);

            // Then
            assertTrue(removed.contains("ttl"), "Listener should receive the removed key after TTL expiration");
        }
    }

    @Nested
    class IterationAndSummary
    {
        /**
         * Verifies that the forEach call transfers only unexpired records.
         */
        @Test
        void shouldReturnOnlyValidEntriesOnForEach()
        {
            // Given
            assertTrue(engine.set("kal", "value"), "Set without TTL should succeed");
            assertTrue(engine.set("git", "value", Duration.ofMillis(10)), "Set with TTL should succeed");
            sleep(40);

            // When
            List<String> keys = new ArrayList<>();
            engine.forEachEntry((key, value, expireAt) -> keys.add(key));

            // Then
            assertEquals(List.of("kal"), keys, "Only non-expired key should be returned by forEachEntry");
        }

        /**
         * Shows that the clear operation empties all segments.
         */
        @Test
        void shouldRemoveAllSegmentsOnClear()
        {
            // Given
            assertTrue(engine.set("a", "1"), "Set A should succeed");
            assertTrue(engine.set("b", "2"), "Set B should succeed");

            // When
            engine.clear();

            // Then
            assertEquals(0, engine.size(), "Size should be 0 after clear");
            List<String> keys = new ArrayList<>();
            engine.forEachEntry((key, value, expireAt) -> keys.add(key));
            assertTrue(keys.isEmpty(), "forEachEntry should yield no keys after clear");
        }

        @Test
        void shouldHonorExactCapacityWhenCapacityIsNotDivisibleBySegments()
        {
            try (CacheEngine<String, String> bounded = newEngine(3, 5)) {
                for (int i = 0; i < 20; i++) {
                    assertTrue(bounded.set("key-" + i, "value-" + i));
                }
                assertEquals(5, bounded.size(), "Segment capacities must add up to the configured maximum");
            }
        }

        @Test
        void shouldNotExceedCapacityWhenSegmentsOutnumberEntries()
        {
            try (CacheEngine<String, String> bounded = newEngine(8, 2)) {
                for (int i = 0; i < 20; i++) {
                    assertTrue(bounded.set("small-" + i, "value-" + i));
                }
                assertEquals(2, bounded.size(), "Effective segment count must not exceed maximum capacity");
            }
        }

        /**
         * Verifies that the fingerprint result remains stable even if the insertion order changes.
         */
        @Test
        void shouldKeepFingerprintStableAcrossReorder()
        {
            // Given
            assertTrue(engine.set("one", "1"));
            assertTrue(engine.set("two", "2"));
            long first = engine.fingerprint();

            // When
            assertTrue(engine.delete("one"));
            assertTrue(engine.set("one", "1"));
            long second = engine.fingerprint();

            // Then
            assertEquals(first, second, "Fingerprint should remain stable after deleting and re-inserting the same key/value");
        }
    }

    private static void sleep(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    private CacheEngine<String, String> newEngine(int segments, int maxCapacity)
    {
        return CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                .segments(segments)
                .maxCapacity(maxCapacity)
                .cleanerPollMillis(5)
                .vertx(vertx)
                .build();
    }

    private void replaceEngine(CacheEngine<String, String> replacement)
    {
        engine.close();
        engine = replacement;
    }

    private static final class RecordingBroker extends Broker
    {
        private final CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();

        @Override
        public void publish(String topic, byte[] payload)
        {
            String value = payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
            events.add(topic + ':' + value);
        }

        @Override
        public void close()
        {
            events.clear();
        }

        List<String> events()
        {
            return events;
        }
    }
}
