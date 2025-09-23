package com.can.config;

import com.can.cluster.ClusterClient;
import com.can.cluster.ClusterState;
import com.can.cluster.ConsistentHashRing;
import com.can.cluster.HashFn;
import com.can.cluster.HintedHandoffService;
import com.can.cluster.Node;
import com.can.cluster.coordination.CoordinationService;
import com.can.codec.StringCodec;
import com.can.core.CacheEngine;
import com.can.core.EvictionPolicyType;
import com.can.metric.MetricsRegistry;
import com.can.rdb.SnapshotFile;
import com.can.pubsub.Broker;
import io.quarkus.arc.DefaultBean;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final AtomicBoolean ownsVertx = new AtomicBoolean(false);

    @Inject
    public AppConfig(AppProperties properties) {
        this.properties = properties;
    }

    @Produces
    @Singleton
    @DefaultBean
    public Vertx vertx()
    {
        ownsVertx.set(true);
        var network = properties.network();

        VertxOptions options = new VertxOptions();

        int eventLoopThreads = network.eventLoopThreads();
        if (eventLoopThreads <= 0) {
            eventLoopThreads = VertxOptions.DEFAULT_EVENT_LOOP_POOL_SIZE;
        }
        options.setEventLoopPoolSize(eventLoopThreads);

        int workerThreads = Math.max(1, network.workerThreads());
        options.setWorkerPoolSize(workerThreads);

        if (shouldPreferNativeTransport()) {
            options.setPreferNativeTransport(true);
        }

        return Vertx.vertx(options);
    }

    void disposeVertx(@Disposes Vertx vertx)
    {
        if (ownsVertx.get()) {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    private boolean shouldPreferNativeTransport()
    {
        String osName = System.getProperty("os.name", "");
        if (osName == null || !osName.toLowerCase(Locale.ROOT).contains("linux")) {
            return false;
        }

        return isNativeTransportAvailable("io.netty.channel.epoll.Epoll")
                || isNativeTransportAvailable("io.netty.incubator.channel.uring.IOUring");
    }

    private boolean isNativeTransportAvailable(String className)
    {
        try {
            Class<?> clazz = Class.forName(className);
            return (boolean) clazz.getMethod("isAvailable").invoke(null);
        } catch (ReflectiveOperationException | LinkageError e) {
            return false;
        }
    }

    @Produces
    @Singleton
    public MetricsRegistry metricsRegistry() {
        return new MetricsRegistry();
    }

    @Produces
    @Singleton
    public CacheEngine<String, String> cacheEngine(
            MetricsRegistry metrics,
            Broker broker,
            SnapshotFile<String, String> snapshotFile
    ) {
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
    public SnapshotFile<String, String> snapshotFile()
    {
        var rdbProps = properties.rdb();
        return new SnapshotFile<>(
                new File(rdbProps.path()),
                StringCodec.UTF8
        );
    }

    @Produces
    @Singleton
    public ConsistentHashRing<Node<String, String>> ring()
    {
        HashFn hash = Arrays::hashCode;
        return new ConsistentHashRing<>(hash, properties.cluster().virtualNodes());
    }

    @Produces
    @Singleton
    public Node<String, String> localNode(CacheEngine<String, String> engine)
    {
        var discovery = properties.cluster().discovery();
        var replication = properties.cluster().replication();
        String nodeId = discovery.nodeId()
                .filter(id -> !id.isBlank())
                .orElseGet(() -> {
                    String host = replication.advertiseHost();
                    if (host == null || host.isBlank() || "0.0.0.0".equals(host)) {
                        host = replication.bindHost();
                    }
                    if (host == null || host.isBlank() || "0.0.0.0".equals(host)) {
                        host = "127.0.0.1";
                    }
                    return host + ":" + replication.port();
                });
        final String resolvedId = nodeId;
        return new Node<>() {
            @Override
            public boolean set(String k, String v, Duration ttl) {
                return engine.set(k, v, ttl);
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
            public boolean compareAndSwap(String k, String v, long expectedCas, Duration ttl) {
                return engine.compareAndSwap(k, v, expectedCas, ttl);
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
    public ClusterState clusterState(Node<String, String> localNode, MetricsRegistry metrics)
    {
        return new ClusterState(localNode.id(), metrics);
    }

    @Produces
    @Singleton
    public HintedHandoffService hintedHandoffService(MetricsRegistry metrics)
    {
        return new HintedHandoffService(metrics);
    }

    @Produces
    @Singleton
    public ClusterClient clusterClient(
            ConsistentHashRing<Node<String, String>> ring,
            CoordinationService coordinationService,
            HintedHandoffService hintedHandoffService
    ) {
        return new ClusterClient(ring, properties.cluster().replicationFactor(), StringCodec.UTF8,
                hintedHandoffService);
    }
}
