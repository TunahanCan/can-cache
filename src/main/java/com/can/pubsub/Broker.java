package com.can.pubsub;

import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Uygulama içinde hafif bir yayınla-abone ol mekanizması sağlayarak önbellek
 * olaylarının diğer bileşenlere iletilmesini kolaylaştırır. Sanal thread'ler
 * üzerinden her mesajı abonelere asenkron iletir ve abonelik yaşam döngüsünü
 * yönetir.
 */
public final class Broker implements AutoCloseable
{
    private final ConcurrentMap<String, CopyOnWriteArrayList<Consumer<byte[]>>> subs = new ConcurrentHashMap<>();
    private final ExecutorService exec = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

    public void publish(String topic, byte[] payload)
    {
        var list = subs.get(topic);
        if (list == null) return;
        for (var c : list) exec.submit(() -> c.accept(payload));
    }

    public AutoCloseable subscribe(String topic, Consumer<byte[]> consumer)
    {
        subs.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(consumer);
        return () -> subs.getOrDefault(topic, new CopyOnWriteArrayList<>()).remove(consumer);
    }

    @Override
    public void close() { exec.shutdownNow(); subs.clear(); }
}