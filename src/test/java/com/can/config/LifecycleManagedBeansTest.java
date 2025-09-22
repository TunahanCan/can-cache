package com.can.config;

import com.can.metric.MetricsReporter;
import com.can.pubsub.Broker;
import com.can.rdb.SnapshotScheduler;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class LifecycleManagedBeansTest {

    @Inject
    MetricsReporter reporter;

    @Inject
    SnapshotScheduler scheduler;

    @Inject
    Broker broker;

    @Inject
    AppProperties properties;

    @Test
    void metricsReporterPreDestroyStopsScheduling() {
        assertTrue(reporter.isRunning());
        reporter.close();
        assertFalse(reporter.isRunning());
        reporter.start(properties.metrics().reportIntervalSeconds());
    }

    @Test
    void snapshotSchedulerPreDestroyStopsScheduling() {
        assertTrue(scheduler.isRunning());
        scheduler.close();
        assertFalse(scheduler.isRunning());
        scheduler.start();
    }

    @Test
    void brokerPreDestroyStopsExecutor() throws Exception {
        assertTrue(broker.isRunning());
        broker.close();
        assertFalse(broker.isRunning());
        try (AutoCloseable ignored = broker.subscribe("lifecycle", payload -> { })) {
            assertTrue(broker.isRunning());
        }
    }
}
