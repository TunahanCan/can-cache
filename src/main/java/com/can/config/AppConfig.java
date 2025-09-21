package com.can.config;

import com.can.cluster.ClusterClient;
import com.can.cluster.ConsistentHashRing;
import com.can.cluster.HashFn;
import com.can.cluster.Node;
import com.can.codec.StringCodec;
import com.can.core.CacheEngine;
import com.can.core.EvictionPolicyType;
import com.can.metric.MetricsRegistry;
import com.can.metric.MetricsReporter;
import com.can.rdb.SnapshotFile;
import com.can.rdb.SnapshotScheduler;
import com.can.pubsub.Broker;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;

@ApplicationScoped
public class AppConfig {

    private final AppProperties properties;

    @Inject
    public AppConfig(AppProperties properties) {
        this.properties = properties;
    }

    @Produces
    @Singleton
    public MetricsRegistry metricsRegistry() {
        return new MetricsRegistry();
    }

    @Produces
    @Singleton
    public MetricsReporter metricsReporter(MetricsRegistry registry) {
        long interval = properties.metrics().reportIntervalSeconds();
        MetricsReporter reporter = new MetricsReporter(registry);
        reporter.start(interval);
        return reporter;
    }

    void disposeMetricsReporter(@Disposes MetricsReporter reporter) {
        reporter.close();
    }

    @Produces
    @Singleton
    public Broker broker() {
        return new Broker();
    }

    void disposeBroker(@Disposes Broker broker) {
        broker.close();
    }

    @Produces
    @Singleton
    public CacheEngine<String, String> cacheEngine(MetricsRegistry metrics, Broker broker, SnapshotFile<String, String> snapshotFile) {
        var cacheProps = properties.cache();
        CacheEngine<String, String> engine =
                CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                .segments(cacheProps.segments())
                .maxCapacity(cacheProps.maxCapacity())
                .cleanerPollMillis(cacheProps.cleanerPollMillis())
                .evictionPolicy(EvictionPolicyType.fromConfig(cacheProps.evictionPolicy()))
                .metrics(metrics)
                .broker(broker)
                .build();

        snapshotFile.load(engine);
        return engine;
    }

    void disposeCacheEngine(@Disposes CacheEngine<String, String> engine) {
        engine.close();
    }

    @Produces
    @Singleton
    public SnapshotFile<String, String> snapshotFile() {
        var rdbProps = properties.rdb();
        return new SnapshotFile<>(
                new File(rdbProps.path()),
                StringCodec.UTF8
        );
    }

    @Produces
    @Singleton
    public SnapshotScheduler<String, String> snapshotScheduler(CacheEngine<String, String> engine,
                                                               SnapshotFile<String, String> snapshotFile) {
        long interval = properties.rdb().snapshotIntervalSeconds();
        SnapshotScheduler<String, String> scheduler = new SnapshotScheduler<>(engine, snapshotFile, interval);
        scheduler.start();
        return scheduler;
    }

    void disposeSnapshotScheduler(@Disposes SnapshotScheduler<String, String> scheduler) {
        scheduler.close();
    }

    @Produces
    @Singleton
    public ConsistentHashRing<Node<String, String>> ring() {
        HashFn hash = key -> Arrays.hashCode(key);
        return new ConsistentHashRing<>(hash, properties.cluster().virtualNodes());
    }

    @Produces
    @Singleton
    public Node<String, String> localNode(CacheEngine<String, String> engine) {
        return new Node<>() {
            @Override
            public void set(String k, String v, Duration ttl) {
                engine.set(k, v, ttl);
            }

            @Override
            public String get(String k) {
                return engine.get(k);
            }

            @Override
            public boolean delete(String k) {
                return engine.delete(k);
            }

            @Override
            public String id() {
                return "node-1";
            }
        };
    }

    @Produces
    @Singleton
    public ClusterClient<String, String> clusterClient(
            ConsistentHashRing<Node<String, String>> ring,
            Node<String, String> localNode
    ) {
        ring.addNode(localNode, localNode.id().getBytes(StandardCharsets.UTF_8));
        return new ClusterClient<>(ring, properties.cluster().replicationFactor(), StringCodec.UTF8);
    }
}
