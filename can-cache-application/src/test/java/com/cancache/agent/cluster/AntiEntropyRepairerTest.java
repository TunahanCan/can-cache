package com.cancache.agent.cluster;

import com.cancache.agent.codec.StringCodec;
import com.cancache.agent.core.CacheEngine;
import com.cancache.agent.core.StoredValueCodec;
import com.cancache.agent.metric.MetricsRegistry;
import com.cancache.agent.pubsub.Broker;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AntiEntropyRepairerTest
{
    private Vertx vertx;
    private CacheEngine<String, String> engine;
    private MetricsRegistry metrics;
    private ConsistentHashRing<Node<String, String>> ring;
    private FakeNode local;
    private FakeNode replica1;
    private FakeNode replica2;
    private AntiEntropyRepairer repairer;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        metrics = new MetricsRegistry();
        engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                .segments(2)
                .maxCapacity(16)
                .cleanerPollMillis(100)
                .metrics(metrics)
                .broker(new NoopBroker())
                .vertx(vertx)
                .build();
        ring = new ConsistentHashRing<>(new ControlledHash(), 1);
        local = new FakeNode("leader");
        replica1 = new FakeNode("replica1");
        replica2 = new FakeNode("replica2");
        ring.addNode(local, bytes("leader"));
        ring.addNode(replica1, bytes("replica1"));
        ring.addNode(replica2, bytes("replica2"));
        repairer = new AntiEntropyRepairer(ring, local, engine, 3, metrics);
    }

    @AfterEach
    void cleanup()
    {
        if (engine != null) {
            engine.close();
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    @Test
    void shouldRepairMissingRemoteReplicasFromLocalSnapshot()
    {
        long expireAt = System.currentTimeMillis() + 5_000L;
        String encoded = encodedValue("local-value", 3, 11L, expireAt);
        engine.set("clientKey", encoded, Duration.ofMillis(5_000L));

        repairer.runOnce();

        assertAll(
                () -> assertEquals(encoded, replica1.storedValue()),
                () -> assertEquals(encoded, replica2.storedValue()),
                () -> assertNotNull(replica1.lastTtl()),
                () -> assertTrue(replica1.lastTtl().toMillis() > 0),
                () -> assertEquals(2L, metrics.counter("anti_entropy_repairs_total").get()),
                () -> assertEquals(1L, metrics.counter("anti_entropy_runs_total").get())
        );
    }

    @Test
    void shouldNotOverwriteDivergentRemoteReplica()
    {
        String localValue = encodedValue("local-value", 3, 11L, 0L);
        String divergentValue = encodedValue("remote-value", 3, 12L, 0L);
        engine.set("clientKey", localValue);
        replica1.preset(divergentValue);

        repairer.runOnce();

        assertAll(
                () -> assertEquals(divergentValue, replica1.storedValue()),
                () -> assertEquals(localValue, replica2.storedValue()),
                () -> assertEquals(1L, metrics.counter("anti_entropy_repairs_total").get()),
                () -> assertEquals(1L, metrics.counter("anti_entropy_conflicts_total").get())
        );
    }

    private static byte[] bytes(String value)
    {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String encodedValue(String value, int flags, long cas, long expireAt)
    {
        return StoredValueCodec.encode(new StoredValueCodec.StoredValue(
                value.getBytes(StandardCharsets.UTF_8), flags, cas, expireAt));
    }

    private static final class ControlledHash implements HashFn
    {
        @Override
        public int hash(byte[] keyBytes)
        {
            String text = new String(keyBytes, StandardCharsets.UTF_8);
            int vnode = 0;
            int idx = text.indexOf('#');
            if (idx >= 0) {
                vnode = Integer.parseInt(text.substring(idx + 1));
                text = text.substring(0, idx);
            }
            return switch (text) {
                case "leader" -> 100 + vnode;
                case "replica1" -> 200 + vnode;
                case "replica2" -> 300 + vnode;
                case "clientKey" -> 50;
                default -> text.hashCode();
            };
        }
    }

    private static final class FakeNode implements Node<String, String>
    {
        private final String id;
        private String storedValue;
        private Duration lastTtl;

        private FakeNode(String id)
        {
            this.id = id;
        }

        void preset(String value)
        {
            storedValue = value;
        }

        String storedValue()
        {
            return storedValue;
        }

        Duration lastTtl()
        {
            return lastTtl;
        }

        @Override
        public boolean set(String key, String value, Duration ttl)
        {
            storedValue = value;
            lastTtl = ttl;
            return true;
        }

        @Override
        public String get(String key)
        {
            return storedValue;
        }

        @Override
        public boolean delete(String key)
        {
            storedValue = null;
            return true;
        }

        @Override
        public boolean compareAndSwap(String key, String value, long expectedCas, Duration ttl)
        {
            return false;
        }

        @Override
        public void clear()
        {
            storedValue = null;
        }

        @Override
        public String id()
        {
            return id;
        }
    }

    private static final class NoopBroker extends Broker
    {
        @Override
        public void publish(String topic, byte[] payload)
        {
        }

        @Override
        public void close()
        {
        }
    }
}
