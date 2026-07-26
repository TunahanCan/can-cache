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
    private final AtomicLong totalConnections = new AtomicLong();
    private final AtomicLong rejectedConnections = new AtomicLong();
    private final AtomicLong dialFailures = new AtomicLong();
    private final AtomicLong failovers = new AtomicLong();
    private final AtomicLong idleTimeouts = new AtomicLong();

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
        totalConnections.incrementAndGet();
        return activeConnections.incrementAndGet();
    }

    public int decActiveConnections() {
        return activeConnections.updateAndGet(current -> Math.max(0, current - 1));
    }

    public int activeConnections() {
        return activeConnections.get();
    }

    public long totalConnections() {
        return totalConnections.get();
    }

    public long incRejectedConnections() {
        return rejectedConnections.incrementAndGet();
    }

    public long rejectedConnections() {
        return rejectedConnections.get();
    }

    public long incDialFailures() {
        return dialFailures.incrementAndGet();
    }

    public long dialFailures() {
        return dialFailures.get();
    }

    public long incFailovers() {
        return failovers.incrementAndGet();
    }

    public long failovers() {
        return failovers.get();
    }

    public long incIdleTimeouts() {
        return idleTimeouts.incrementAndGet();
    }

    public long idleTimeouts() {
        return idleTimeouts.get();
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
