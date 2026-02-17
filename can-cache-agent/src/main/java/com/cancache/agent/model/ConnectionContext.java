package com.cancache.agent.model;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public final class ConnectionContext
{
    private final Instant startTime = Instant.now();
    private final String clientAddr;
    private final String upstreamAddr;
    private final AtomicLong bytesIn = new AtomicLong();
    private final AtomicLong bytesOut = new AtomicLong();

    public ConnectionContext(String clientAddr, String upstreamAddr) {
        this.clientAddr = clientAddr;
        this.upstreamAddr = upstreamAddr;
    }

    public Instant startTime() {
        return startTime;
    }

    public String clientAddr() {
        return clientAddr;
    }

    public String upstreamAddr() {
        return upstreamAddr;
    }

    public long addBytesIn(long n) {
        return bytesIn.addAndGet(n);
    }

    public long addBytesOut(long n) {
        return bytesOut.addAndGet(n);
    }

    public long bytesIn() {
        return bytesIn.get();
    }

    public long bytesOut() {
        return bytesOut.get();
    }
}
