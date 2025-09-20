package com.can.pubsub;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

/** Minimal in-process Pub/Sub; virtual thread per task. */
public final class Broker implements AutoCloseable
{
    private final ConcurrentMap<String, CopyOnWriteArrayList<Consumer<byte[]>>> subs = new ConcurrentHashMap<>();
    private final ExecutorService exec = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

    public void publish(String topic, byte[] payload) {
        var list = subs.get(topic);
        if (list == null) return;
        for (var c : list) exec.submit(() -> c.accept(payload));
    }

    public AutoCloseable subscribe(String topic, Consumer<byte[]> consumer) {
        subs.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(consumer);
        return () -> subs.getOrDefault(topic, new CopyOnWriteArrayList<>()).remove(consumer);
    }

    @Override public void close() { exec.shutdownNow(); subs.clear(); }
}