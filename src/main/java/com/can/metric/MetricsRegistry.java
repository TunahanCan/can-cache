package com.can.metric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MetricsRegistry {
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();

    public Counter counter(String name) { return counters.computeIfAbsent(name, Counter::new); }
    public Timer timer(String name) { return timers.computeIfAbsent(name, Timer::new); }

    public Map<String, Counter> counters(){ return counters; }
    public Map<String, Timer> timers(){ return timers; }
}