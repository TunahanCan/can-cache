package com.cancache.agent.model;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class NodeStats {
    private final UpstreamAddress address;
    private final AtomicReference<UpstreamState> state = new AtomicReference<>(UpstreamState.UNKNOWN);
    private volatile Instant lastCheck = Instant.EPOCH;
    private volatile String lastError = "-";

    private final AtomicInteger activeConn = new AtomicInteger();
    private final AtomicInteger pendingConn = new AtomicInteger();
    private final AtomicInteger passiveFailures = new AtomicInteger();
    private final AtomicLong totalConn = new AtomicLong();
    private final AtomicLong bytesIn = new AtomicLong();
    private final AtomicLong bytesOut = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();

    public NodeStats(String address) {
        this(UpstreamAddress.parse(address));
    }

    public NodeStats(UpstreamAddress address) {
        this.address = address;
    }

    public String address() {
        return address.toString();
    }

    public UpstreamAddress upstreamAddress() {
        return address;
    }

    public UpstreamState state() {
        return state.get();
    }

    public void state(UpstreamState state) {
        this.state.set(state);
    }

    public boolean compareAndSetState(UpstreamState expected, UpstreamState next) {
        return state.compareAndSet(expected, next);
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
        return activeConn.updateAndGet(current -> Math.max(0, current - 1));
    }

    public int activeConn() {
        return activeConn.get();
    }

    public int reservePendingConnection() {
        return pendingConn.incrementAndGet();
    }

    public int releasePendingConnection() {
        return pendingConn.updateAndGet(current -> Math.max(0, current - 1));
    }

    public int pendingConn() {
        return pendingConn.get();
    }

    public int load() {
        return activeConn.get() + pendingConn.get();
    }

    public boolean recordPassiveFailure(int threshold) {
        int failures = passiveFailures.incrementAndGet();
        if (failures >= Math.max(1, threshold)) {
            state.set(UpstreamState.DOWN);
            return true;
        }
        return false;
    }

    public void clearPassiveFailures() {
        passiveFailures.set(0);
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
