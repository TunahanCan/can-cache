package com.can.core;

import com.can.codec.StringCodec;
import com.can.metric.MetricsRegistry;
import com.can.metric.Timer;
import com.can.pubsub.Broker;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class CacheEngineTest
{
    private CacheEngine<String, String> engine;
    private Vertx vertx;
    private WorkerExecutor worker;
    private MetricsRegistry metrics;
    private RecordingBroker broker;

    @BeforeEach
    void setup()
    {
        metrics = new MetricsRegistry();
        broker = new RecordingBroker();
        vertx = Vertx.vertx();
        worker = vertx.createSharedWorkerExecutor("cache-engine-test");
        engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                .segments(2)
                .maxCapacity(8)
                .cleanerPollMillis(5)
                .metrics(metrics)
                .broker(broker)
                .vertx(vertx)
                .workerExecutor(worker)
                .build();
    }

    @AfterEach
    void cleanup()
    {
        engine.close();
        worker.close();
        vertx.close().toCompletionStage().toCompletableFuture().join();
    }

    @Nested
    class SetGetOperations
    {
        @Test
        void set_and_get_roundtrip_updates_metrics()
        {
            assertTrue(engine.set("key", "value"));
            assertEquals("value", engine.get("key"));

            assertEquals(1L, metrics.counter("cache_hits").get());
            Timer.Sample sample = metrics.timer("cache_get").snapshot();
            assertEquals("cache_get", sample.name());
            assertTrue(sample.count() > 0);
            assertTrue(broker.events().contains("keyspace:set:key"));
        }

        @Test
        void delete_removes_entry_and_records_timer()
        {
            engine.set("key", "value");
            assertTrue(engine.delete("key"));
            assertFalse(engine.exists("key"));

            Timer.Sample sample = metrics.timer("cache_del").snapshot();
            assertEquals("cache_del", sample.name());
            assertTrue(sample.count() > 0);
            assertTrue(broker.events().contains("keyspace:del:key"));
        }

        @Test
        void get_with_expired_value_triggers_miss_and_delete() throws InterruptedException
        {
            assertTrue(engine.set("key", "value", Duration.ofMillis(10)));
            Thread.sleep(40);

            assertNull(engine.get("key"));
            assertFalse(engine.exists("key"));
            assertTrue(metrics.counter("cache_misses").get() > 0);
        }

        @Test
        void set_allows_empty_string_values()
        {
            assertTrue(engine.set("key", ""));
            assertEquals("", engine.get("key"));
        }

        @Test
        void set_handles_large_ttl_without_overflow()
        {
            long now = System.currentTimeMillis();
            Duration ttl = Duration.ofMillis(Long.MAX_VALUE - now + 1000);

            assertTrue(engine.set("key", "value", ttl));
            assertEquals("value", engine.get("key"));
            assertTrue(engine.exists("key"));
        }
    }

    @Nested
    class CasOperations
    {
        @Test
        void compare_and_swap_updates_value_and_ttl() throws InterruptedException
        {
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue("v1".getBytes(StandardCharsets.UTF_8), 1, 7L, 0L);
            String encoded = StoredValueCodec.encode(base);
            engine.set("key", encoded);

            StoredValueCodec.StoredValue updated = base.withValue("v2".getBytes(StandardCharsets.UTF_8), 9L);
            String next = StoredValueCodec.encode(updated);

            assertTrue(engine.compareAndSwap("key", next, 7L, Duration.ofMillis(30)));
            assertEquals(next, engine.get("key"));
            assertTrue(engine.exists("key"));
            Thread.sleep(60);
            assertFalse(engine.exists("key"));
            assertTrue(broker.events().stream().anyMatch(e -> e.startsWith("keyspace:set:key")));
        }

        @Test
        void compare_and_swap_rejects_when_cas_mismatch()
        {
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue("v1".getBytes(StandardCharsets.UTF_8), 1, 7L, 0L);
            engine.set("key", StoredValueCodec.encode(base));

            assertFalse(engine.compareAndSwap("key", "ignored", 1L, null));
            assertEquals(StoredValueCodec.encode(base), engine.get("key"));
        }
    }

    @Nested
    class ReplayOperations
    {
        @Test
        void replay_set_restores_entry()
        {
            byte[] op = new byte[]{'S'};
            engine.replay(op, StringCodec.UTF8.encode("key"), StringCodec.UTF8.encode("value"), 0L);

            assertEquals("value", engine.get("key"));
        }

        @Test
        void replay_delete_removes_entry()
        {
            engine.set("key", "value");
            byte[] op = new byte[]{'D'};
            engine.replay(op, StringCodec.UTF8.encode("key"), new byte[0], 0L);

            assertNull(engine.get("key"));
        }

        @Test
        void replay_skips_expired_entry()
        {
            byte[] op = new byte[]{'S'};
            engine.replay(op, StringCodec.UTF8.encode("key"), StringCodec.UTF8.encode("value"), System.currentTimeMillis() - 1000);

            assertNull(engine.get("key"));
        }
    }

    @Nested
    class ListenerOperations
    {
        @Test
        void on_removal_invoked_for_manual_delete()
        {
            engine.set("key", "value");
            List<String> removed = new ArrayList<>();
            AutoCloseable handle = engine.onRemoval(removed::add);

            assertTrue(engine.delete("key"));
            assertEquals(List.of("key"), removed);

            assertDoesNotThrow(() -> handle.close());
        }

        @Test
        void on_removal_invoked_for_ttl_expiration() throws InterruptedException
        {
            List<String> removed = new ArrayList<>();
            engine.onRemoval(removed::add);
            engine.set("key", "value", Duration.ofMillis(10));
            Thread.sleep(60);

            assertTrue(removed.contains("key"));
        }
    }

    @Nested
    class IterationOperations
    {
        @Test
        void for_each_entry_skips_expired_values()
        {
            engine.set("keep", "value");
            engine.set("expire", "soon", Duration.ofMillis(10));
            List<String> keys = new ArrayList<>();
            engine.forEachEntry((key, value, expireAt) -> keys.add(key));

            assertTrue(keys.contains("keep"));
            assertTrue(keys.contains("expire"));

            // Wait and ensure expired entry is no longer provided
            try
            {
                Thread.sleep(40);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            keys.clear();
            engine.forEachEntry((key, value, expireAt) -> keys.add(key));
            assertEquals(List.of("keep"), keys);
        }

        @Test
        void clear_resets_all_segments()
        {
            engine.set("a", "1");
            engine.set("b", "2");
            engine.clear();

            assertEquals(0, engine.size());
            List<String> keys = new ArrayList<>();
            engine.forEachEntry((key, value, expireAt) -> keys.add(key));
            assertTrue(keys.isEmpty());
        }
    }

    private static final class RecordingBroker extends Broker
    {
        private final CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();

        @Override
        public void publish(String topic, byte[] payload)
        {
            String value = payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
            events.add(topic + ":" + value);
        }

        List<String> events()
        {
            return events;
        }
    }
}
