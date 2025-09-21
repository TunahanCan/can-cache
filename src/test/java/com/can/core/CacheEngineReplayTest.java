package com.can.core;

import com.can.codec.StringCodec;
import com.can.pubsub.Broker;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CacheEngineReplayTest {

    @Test
    void replaySetSkipsSideEffects() throws Exception {
        var codec = StringCodec.UTF8;
        AtomicInteger published = new AtomicInteger();
        try (Broker broker = new Broker();
             AutoCloseable ignored = broker.subscribe("keyspace:set", payload -> published.incrementAndGet());
             CacheEngine<String, String> engine = CacheEngine.<String, String>builder(codec, codec)
                     .broker(broker)
                     .build()) {
            long expireAt = System.currentTimeMillis() + 5_000;
            engine.replay(new byte[]{'S'}, codec.encode("foo"), codec.encode("bar"), expireAt);
            assertEquals("bar", engine.get("foo"));
            Thread.sleep(50);
            assertEquals(0, published.get());
        }
    }

    @Test
    void replaySkipsExpiredEntriesAndRemovesExistingValue() throws Exception {
        var codec = StringCodec.UTF8;
        try (CacheEngine<String, String> engine = CacheEngine.<String, String>builder(codec, codec).build()) {
            engine.set("foo", "live");
            long expiredAt = System.currentTimeMillis() - 1_000;
            engine.replay(new byte[]{'S'}, codec.encode("foo"), codec.encode("stale"), expiredAt);
            assertEquals(0, engine.size());
            assertFalse(engine.exists("foo"));
            assertNull(engine.get("foo"));
        }
    }
}
