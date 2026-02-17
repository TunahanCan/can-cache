package com.cancache.agent.service;

import jakarta.enterprise.context.ApplicationScoped;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class MetricsModel {

    private final Instant startedAt = Instant.now();
    private final AtomicLong bytesIn = new AtomicLong();
    private final AtomicLong bytesOut = new AtomicLong();
    private final AtomicInteger activeConnections = new AtomicInteger();
    private final AtomicLong dnsChanges = new AtomicLong();

    private final ArrayDeque<String> events = new ArrayDeque<>();

    public Instant startedAt() {
        return startedAt;
    }

    public long addBytesIn(long amount) {
        return bytesIn.addAndGet(amount);
    }

    public long addBytesOut(long amount) {
        return bytesOut.addAndGet(amount);
    }

    public long bytesIn() {
        return bytesIn.get();
    }

    public long bytesOut() {
        return bytesOut.get();
    }

    public int incActiveConnections() {
        return activeConnections.incrementAndGet();
    }

    public int decActiveConnections() {
        return activeConnections.decrementAndGet();
    }

    public int activeConnections() {
        return activeConnections.get();
    }

    public long incDnsChanges() {
        return dnsChanges.incrementAndGet();
    }

    public long dnsChanges() {
        return dnsChanges.get();
    }

    public synchronized void addEvent(String event) {
        events.addFirst(event);
        while (events.size() > 15) {
            events.removeLast();
        }
    }

    public synchronized List<String> latestEvents() {
        return new ArrayList<>(events);
    }
}
