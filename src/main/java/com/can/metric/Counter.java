package com.can.metric;

import java.util.concurrent.atomic.AtomicLong;

public final class Counter
{
    private final String name;
    private final AtomicLong value = new AtomicLong();
    public Counter(String name) { this.name = name; }
    public void inc() { value.incrementAndGet(); }
    public void add(long delta) { value.addAndGet(delta); }
    public long get() { return value.get(); }
    public String name() { return name; }
}