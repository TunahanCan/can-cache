package com.can.pubsub;

import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Uygulama içinde hafif bir yayınla-abone ol mekanizması sağlayarak önbellek
 * olaylarının diğer bileşenlere iletilmesini kolaylaştırır. Sanal thread'ler
 * üzerinden her mesajı abonelere asenkron iletir ve abonelik yaşam döngüsünü
 * yönetir.
 */
@Startup
@Singleton
public class Broker implements AutoCloseable {

    private final ConcurrentMap<String, CopyOnWriteArrayList<Consumer<byte[]>>> subscriptions = new ConcurrentHashMap<>();
    private volatile ExecutorService executor;

    @PostConstruct
    void init() {
        ensureExecutor();
    }

    public void publish(String topic, byte[] payload) {
        var list = subscriptions.get(topic);
        if (list == null || list.isEmpty()) {
            return;
        }
        ExecutorService current = ensureExecutor();
        for (var consumer : list) {
            current.submit(() -> consumer.accept(payload));
        }
    }


    public boolean isRunning() {
        ExecutorService current = executor;
        return current != null && !current.isShutdown();
    }

    @PreDestroy
    void shutdown() {
        close();
    }

    @Override
    public synchronized void close() {
        ExecutorService current = executor;
        if (current != null) {
            current.shutdownNow();
            executor = null;
        }
        subscriptions.clear();
    }

    private synchronized ExecutorService ensureExecutor() {
        ExecutorService current = executor;
        if (current == null || current.isShutdown()) {
            executor = current = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
        }
        return current;
    }
}
