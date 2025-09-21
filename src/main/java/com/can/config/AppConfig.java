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
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Configuration
@EnableConfigurationProperties(AppProperties.class)
public class AppConfig {

    private final AppProperties properties;

    public AppConfig(AppProperties properties) {
        this.properties = properties;
    }

    // ---- Metrics ----
    @Bean
    public MetricsRegistry metricsRegistry() {
        return new MetricsRegistry();
    }

    @Bean(destroyMethod = "close")
    public MetricsReporter metricsReporter(MetricsRegistry reg) {
        long interval = properties.getMetrics().getReportIntervalSeconds();
        var reporter = new MetricsReporter(reg);
        reporter.start(interval);
        return reporter;
    }

    // ---- Pub/Sub ----
    @Bean(destroyMethod = "close")
    public Broker broker() { return new Broker(); }

    // ---- AOF ----
    @Bean(destroyMethod = "close")
    public AppendOnlyFile<String,String> aof() {
        var aofProps = properties.getAof();
        return new AppendOnlyFile<>(new File(aofProps.getPath()), StringCodec.UTF8, StringCodec.UTF8, aofProps.isFsyncEvery());
    }

    // ---- Cache Engine ----
    @Bean(destroyMethod = "close")
    public CacheEngine<String,String> cacheEngine(
            AppendOnlyFile<String,String> aof,
            MetricsRegistry metrics,
            Broker broker
    ) {
        var cacheProps = properties.getCache();
        var engine = CacheEngine.<String,String>builder(StringCodec.UTF8, StringCodec.UTF8)
                .segments(cacheProps.getSegments())
                .maxCapacity(cacheProps.getMaxCapacity())
                .cleanerPollMillis(cacheProps.getCleanerPollMillis())
                .evictionPolicy(EvictionPolicyType.fromConfig(cacheProps.getEvictionPolicy()))
                .aof(aof).metrics(metrics).broker(broker)
                .build();

        // Açılışta AOF replay
        AppendOnlyFile.replay(new File(properties.getAof().getPath()), engine);
        return engine;
    }

    // ---- Cluster (tek node; istersen başka node bean'leri ekleyip RF>1 yap) ----
    @Bean
    public ConsistentHashRing<Node<String,String>> ring() {
        HashFn hash = key -> java.util.Arrays.hashCode(key); // basit demo hash
        return new ConsistentHashRing<>(hash, properties.getCluster().getVirtualNodes());
    }

    @Bean
    public Node<String,String> localNode(CacheEngine<String,String> engine) {
        return new Node<>() {
            @Override public void set(String k, String v, java.time.Duration ttl){ engine.set(k,v,ttl); }
            @Override public String get(String k){ return engine.get(k); }
            @Override public boolean delete(String k){ return engine.delete(k); }
            @Override public String id(){ return "node-1"; }
        };
    }

    @Bean
    public ClusterClient<String,String> clusterClient(
            ConsistentHashRing<Node<String,String>> ring,
            Node<String,String> localNode
    ) {
        ring.addNode(localNode, localNode.id().getBytes());
        return new ClusterClient<>(ring, properties.getCluster().getReplicationFactor(), StringCodec.UTF8);
    }
}