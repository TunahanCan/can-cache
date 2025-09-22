package com.can.config;

import com.can.cluster.ClusterClient;
import com.can.cluster.ConsistentHashRing;
import com.can.cluster.HashFn;
import com.can.cluster.Node;
import com.can.cluster.coordination.CoordinationService;
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
import java.time.Duration;
import java.util.Arrays;

/**
 * CDI tarafından yönetilen bu yapılandırma sınıfı, önbellek motoru, metrik
 * bileşenleri, snapshot alma mekanizması ve küme istemcisi gibi uygulamanın
 * çalışması için gerekli tüm tekil bean'leri üretir. Bean oluşturulurken
 * {@link AppProperties} üzerinden okunan değerler kullanılır ve yaşam döngüsü
 * boyunca gerekli kaynakların başlatılıp kapatılmasından sorumludur.
 */
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
        HashFn hash = Arrays::hashCode;
        return new ConsistentHashRing<>(hash, properties.cluster().virtualNodes());
    }

    @Produces
    @Singleton
    public Node<String, String> localNode(CacheEngine<String, String> engine) {
        var discovery = properties.cluster().discovery();
        var replication = properties.cluster().replication();
        String nodeId = discovery.nodeId();
        if (nodeId == null || nodeId.isBlank()) {
            String host = replication.advertiseHost();
            if (host == null || host.isBlank() || "0.0.0.0".equals(host)) {
                host = replication.bindHost();
            }
            if (host == null || host.isBlank() || "0.0.0.0".equals(host)) {
                host = "127.0.0.1";
            }
            nodeId = host + ":" + replication.port();
        }
        final String resolvedId = nodeId;
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
            public void clear() {
                engine.clear();
            }

            @Override
            public String id() {
                return resolvedId;
            }
        };
    }

    @Produces
    @Singleton
    public ClusterClient<String, String> clusterClient(
            ConsistentHashRing<Node<String, String>> ring,
            CoordinationService coordinationService
    ) {
        return new ClusterClient<>(ring, properties.cluster().replicationFactor(), StringCodec.UTF8);
    }
}
