package com.cancache.agent.config;

import com.cancache.agent.cluster.ClusterClient;
import com.cancache.agent.cluster.ClusterState;
import com.cancache.agent.cluster.ConsistentHashRing;
import com.cancache.agent.cluster.HashFn;
import com.cancache.agent.cluster.HintedHandoffService;
import com.cancache.agent.cluster.Node;
import com.cancache.agent.cluster.coordination.CoordinationService;
import com.cancache.agent.cluster.coordination.DiscoveryStrategy; // Import eklendi
import com.cancache.agent.cluster.coordination.MulticastDiscoveryStrategy; // Import eklendi
import com.cancache.agent.cluster.coordination.gossip.GossipDiscoveryStrategy; // Import eklendi
import com.cancache.agent.codec.StringCodec;
import com.cancache.agent.core.CacheEngine;
import com.cancache.agent.core.EvictionPolicyType;
import com.cancache.agent.metric.MetricsRegistry;
import com.cancache.agent.pubsub.Broker;
import io.quarkus.arc.DefaultBean;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.WorkerExecutor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named; // Import eklendi
import jakarta.inject.Singleton;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * CDI tarafından yönetilen bu yapılandırma sınıfı, önbellek motoru, metrik
 * bileşenleri ve küme istemcisi gibi uygulamanın
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
    public WorkerExecutor workerExecutor(Vertx vertx)
    {
        int poolSize = Math.max(4, Runtime.getRuntime().availableProcessors());
        return vertx.createSharedWorkerExecutor("can-cache-worker", poolSize, 1, TimeUnit.MINUTES);
    }

    void disposeWorkerExecutor(@Disposes WorkerExecutor workerExecutor)
    {
        workerExecutor.close();
    }


    @Produces
    @Singleton
    public CacheEngine<String, String> cacheEngine(
            MetricsRegistry metrics,
            Broker broker,
            Vertx vertx)
    {
        var cacheProps = properties.cache();
        return CacheEngine.builder(StringCodec.UTF8, StringCodec.UTF8)
        .segments(cacheProps.segments())
        .maxCapacity(cacheProps.maxCapacity())
        .cleanerPollMillis(cacheProps.cleanerPollMillis())
        .evictionPolicy(EvictionPolicyType.fromConfig(cacheProps.evictionPolicy()))
        .metrics(metrics)
        .broker(broker)
        .vertx(vertx)
        .build();
    }

    void disposeCacheEngine(@Disposes CacheEngine<String, String> engine) {
        engine.close();
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

        final String resolvedId = discovery.nodeId()
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
        return new Node<>()
        {
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
    @Named("selectedDiscoveryStrategy")
    public DiscoveryStrategy discoveryStrategy(Vertx vertx, AppProperties properties, ClusterState clusterState) {
        AppProperties.DiscoveryType discoveryType = properties.cluster().discovery().type();
        switch (discoveryType) {
            case MULTICAST:
                return new MulticastDiscoveryStrategy(vertx, properties, clusterState);
            case GOSSIP:
                return new GossipDiscoveryStrategy(vertx, properties, clusterState);
            case DNS:
                // TODO: Implement DnsDiscoveryStrategy
                throw new UnsupportedOperationException("DNS Discovery Strategy is not yet implemented.");
            default:
                throw new IllegalArgumentException("Unknown discovery type: " + discoveryType);
        }
    }

    // DiscoveryStrategy kaynaklarını kapatmak için dispose metodu
    void disposeDiscoveryStrategy(@Disposes @Named("selectedDiscoveryStrategy") DiscoveryStrategy discoveryStrategy) {
        discoveryStrategy.close();
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
