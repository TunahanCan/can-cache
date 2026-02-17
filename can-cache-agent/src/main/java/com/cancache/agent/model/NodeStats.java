package com.cancache.agent.model;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class NodeStats {
    private final String address;
    private volatile UpstreamState state = UpstreamState.UNKNOWN;
    private volatile Instant lastCheck = Instant.EPOCH;
    private volatile String lastError = "-";

    private final AtomicInteger activeConn = new AtomicInteger();
    private final AtomicLong totalConn = new AtomicLong();
    private final AtomicLong bytesIn = new AtomicLong();
    private final AtomicLong bytesOut = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();

    public NodeStats(String address) {
        this.address = address;
    }

    public String address() {
        return address;
    }

    public UpstreamState state() {
        return state;
    }

    public void state(UpstreamState state) {
        this.state = state;
    }

    public Instant lastCheck() {
        return lastCheck;
    }

    public void markCheck(String error) {
        this.lastCheck = Instant.now();
        this.lastError = error == null ? "-" : error;
    }

    public Duration lastCheckAge() {
        return Duration.between(lastCheck, Instant.now());
    }

    public String lastError() {
        return lastError;
    }

    public int incActiveConn() {
        totalConn.incrementAndGet();
        return activeConn.incrementAndGet();
    }

    public int decActiveConn() {
        return activeConn.decrementAndGet();
    }

    public int activeConn() {
        return activeConn.get();
    }

    public long totalConn() {
        return totalConn.get();
    }

    public long addBytesIn(long value) {
        return bytesIn.addAndGet(value);
    }

    public long addBytesOut(long value) {
        return bytesOut.addAndGet(value);
    }

    public long bytesIn() {
        return bytesIn.get();
    }

    public long bytesOut() {
        return bytesOut.get();
    }

    public long incError() {
        return errorCount.incrementAndGet();
    }

    public long errorCount() {
        return errorCount.get();
    }
}
