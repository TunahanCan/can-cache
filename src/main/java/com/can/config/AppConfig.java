package com.can.config;

import com.can.aof.AppendOnlyFile;
import com.can.cluster.ClusterClient;
import com.can.cluster.ConsistentHashRing;
import com.can.cluster.HashFn;
import com.can.cluster.Node;
import com.can.codec.StringCodec;
import com.can.core.CacheEngine;
import com.can.core.EvictionPolicyType;
import com.can.metric.MetricsRegistry;
import com.can.metric.MetricsReporter;
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
    public CacheEngine<String, String> cacheEngine(MetricsRegistry metrics, Broker broker) {
        var cacheProps = properties.cache();
        var aofProps = properties.aof();

        AppendOnlyFile<String, String> aof = new AppendOnlyFile<>(
                new File(aofProps.path()),
                StringCodec.UTF8,
                StringCodec.UTF8,
                aofProps.fsyncEvery()
        );

        CacheEngine<String, String> engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                .segments(cacheProps.segments())
                .maxCapacity(cacheProps.maxCapacity())
                .cleanerPollMillis(cacheProps.cleanerPollMillis())
                .evictionPolicy(EvictionPolicyType.fromConfig(cacheProps.evictionPolicy()))
                .aof(aof)
                .metrics(metrics)
                .broker(broker)
                .build();

        AppendOnlyFile.replay(new File(aofProps.path()), engine);
        return engine;
    }

    void disposeCacheEngine(@Disposes CacheEngine<String, String> engine) {
        engine.close();
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
