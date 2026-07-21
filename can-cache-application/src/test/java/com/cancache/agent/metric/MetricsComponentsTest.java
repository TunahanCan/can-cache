package com.cancache.agent.metric;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MetricsRegistry, Counter, and Timer components.
 * <p>
 * Note: MetricsReporter now uses the Quarkus Micrometer extension.
 * HTTP endpoint tests should be conducted as integration tests.
 * </p>
 */
class MetricsComponentsTest
{
    @Nested
    class CounterBehavior
    {
        /**
         * Verifies that incrementing and adding properly update the counter's value.
         */
        @Test
        void shouldHandleIncrementAndAdd()
        {
            // Given
            Counter counter = new Counter("hits");
            
            // When
            counter.inc();
            counter.add(4);
            
            // Then
            assertEquals(5, counter.get(), "Counter value should equal the sum of increments");
            assertEquals("hits", counter.name(), "Counter name should match the initialized value");
        }
    }

    @Nested
    class TimerBehavior
    {
        /**
         * Shows that duration records are aggregated into statistics correctly.
         */
        @Test
        void shouldAggregateDurationsIntoStatistics()
        {
            // Given
            Timer timer = new Timer("latency", 128);
            
            // When
            timer.record(1_000);
            timer.record(2_000);
            Timer.Sample sample = timer.snapshot();
            
            // Then
            assertEquals("latency", sample.name(), "Timer sample name should match");
            assertEquals(2, sample.count(), "Count should be 2");
            assertEquals(3_000, sample.totalNs(), "Total duration should be 3000");
            assertEquals(1_000, sample.minNs(), "Minimum duration should be 1000");
            assertEquals(2_000, sample.maxNs(), "Maximum duration should be 2000");
            assertTrue(sample.avgNs() >= 1_000, "Average duration should be reasonable");
        }
    }

    @Nested
    class RegistryBehavior
    {
        /**
         * Verifies that components with the same name are reused by the registry.
         */
        @Test
        void shouldReuseComponentsWithSameName()
        {
            // Given
            MetricsRegistry registry = new MetricsRegistry();
            
            // When
            Counter firstCounter = registry.counter("requests");
            Counter secondCounter = registry.counter("requests");
            Timer firstTimer = registry.timer("latency");
            Timer secondTimer = registry.timer("latency");
            
            // Then
            assertSame(firstCounter, secondCounter, "Repeated counter requests should return the exact same instance");
            assertSame(firstTimer, secondTimer, "Repeated timer requests should return the exact same instance");
            assertTrue(registry.counters().containsKey("requests"), "Registry should contain the counter");
            assertTrue(registry.timers().containsKey("latency"), "Registry should contain the timer");
        }
    }

    @Nested
    class ReporterBehavior
    {
        /**
         * MetricsReporter now operates based on Micrometer.
         * The Quarkus Micrometer extension automatically provides the configured metrics endpoint.
         * HTTP endpoint tests should be conducted as Quarkus integration tests (@QuarkusTest).
         * This test verifies behavior without a MeterRegistry.
         */
        @Test
        void shouldReturnNotRunningWhenNoMeterRegistryProvided()
        {
            // Given
            // No-arg constructor uses null MeterRegistry in test mode
            MetricsReporter reporter = new MetricsReporter();
            
            // When / Then
            assertFalse(reporter.isRunning(), "Reporter should not be running if no meter registry is provided");
            assertEquals(-1, reporter.actualPort(), "Actual port should be -1 when not running");
            
            // Should close safely
            assertDoesNotThrow(reporter::close, "Closing the reporter should not throw exceptions");
        }
    }
}
