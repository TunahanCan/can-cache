package com.cancache.agent.metric;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Micrometer tabanlı metrik kayıt yapısıdır. Quarkus'un sağladığı
 * {@link MeterRegistry} üzerinden Prometheus uyumlu metrikler üretir.
 * Mevcut {@link Counter} ve {@link Timer} arayüzleri korunarak
 * geriye dönük uyumluluk sağlanır.
 */
@ApplicationScoped
public class MetricsRegistry
{
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;

    @Inject
    public MetricsRegistry(MeterRegistry meterRegistry)
    {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Test amaçlı argümansız constructor.
     */
    public MetricsRegistry()
    {
        this.meterRegistry = null;
    }

    public Counter counter(String name)
    {
        return counters.computeIfAbsent(name, n -> {
            if (meterRegistry != null) {
                io.micrometer.core.instrument.Counter micrometerCounter =
                        meterRegistry.counter(n, Tags.empty());
                return new MicrometerCounter(n, micrometerCounter);
            }
            return new Counter(n);
        });
    }

    public Timer timer(String name)
    {
        return timers.computeIfAbsent(name, n -> {
            if (meterRegistry != null) {
                io.micrometer.core.instrument.Timer micrometerTimer =
                        meterRegistry.timer(n, Tags.empty());
                return new MicrometerTimer(n, micrometerTimer);
            }
            return new Timer(n);
        });
    }

    public Map<String, Counter> counters() { return counters; }
    public Map<String, Timer> timers() { return timers; }

    /**
     * Micrometer Counter wrapper - mevcut Counter arayüzüyle uyumlu.
     */
    private static class MicrometerCounter extends Counter
    {
        private final io.micrometer.core.instrument.Counter delegate;

        MicrometerCounter(String name, io.micrometer.core.instrument.Counter delegate)
        {
            super(name);
            this.delegate = delegate;
        }

        @Override
        public void inc()
        {
            super.inc();
            delegate.increment();
        }

        @Override
        public void add(long delta)
        {
            super.add(delta);
            delegate.increment(delta);
        }
    }

    /**
     * Micrometer Timer wrapper - mevcut Timer arayüzüyle uyumlu.
     */
    private static class MicrometerTimer extends Timer
    {
        private final io.micrometer.core.instrument.Timer delegate;

        MicrometerTimer(String name, io.micrometer.core.instrument.Timer delegate)
        {
            super(name);
            this.delegate = delegate;
        }

        @Override
        public void record(long durationNs)
        {
            super.record(durationNs);
            delegate.record(durationNs, TimeUnit.NANOSECONDS);
        }
    }
}