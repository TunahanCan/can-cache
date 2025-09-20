package com.can.config;

import com.can.aof.AppendOnlyFile;
import com.can.cluster.ClusterClient;
import com.can.cluster.ConsistentHashRing;
import com.can.cluster.HashFn;
import com.can.cluster.Node;
import com.can.codec.StringCodec;
import com.can.core.CacheEngine;
import com.can.metric.MetricsRegistry;
import com.can.metric.MetricsReporter;
import com.can.pubsub.Broker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Configuration
public class AppConfig {

    // ---- Metrics ----
    @Bean
    public MetricsRegistry metricsRegistry() {
        return new MetricsRegistry();
    }

    @Bean(destroyMethod = "close")
    public MetricsReporter metricsReporter(
            MetricsRegistry reg,
            @Value("${app.metrics.reportIntervalSeconds:5}") long intervalSec
    ) {
        var r = new MetricsReporter(reg); r.start(intervalSec); return r;
    }

    // ---- Pub/Sub ----
    @Bean(destroyMethod = "close")
    public Broker broker() { return new Broker(); }

    // ---- AOF ----
    @Bean(destroyMethod = "close")
    public AppendOnlyFile<String,String> aof(
            @Value("${app.aof.path:data.aof}") String path,
            @Value("${app.aof.fsyncEvery:true}") boolean fsyncEvery
    ) {
        return new AppendOnlyFile<>(new File(path), StringCodec.UTF8, StringCodec.UTF8, fsyncEvery);
    }

    // ---- Cache Engine ----
    @Bean(destroyMethod = "close")
    public CacheEngine<String,String> cacheEngine(
            @Value("${app.cache.segments:8}") int segments,
            @Value("${app.cache.maxCapacity:10000}") int maxCap,
            @Value("${app.cache.cleanerPollMillis:100}") long pollMs,
            @Value("${app.aof.path:data.aof}") String path,
            AppendOnlyFile<String,String> aof,
            MetricsRegistry metrics,
            Broker broker
    ) {
        var engine = CacheEngine.<String,String>builder(StringCodec.UTF8, StringCodec.UTF8)
                .segments(segments).maxCapacity(maxCap).cleanerPollMillis(pollMs)
                .aof(aof).metrics(metrics).broker(broker)
                .build();

        // Açılışta AOF replay
        AppendOnlyFile.replay(new File(path), engine);
        return engine;
    }

    // ---- Cluster (tek node; istersen başka node bean'leri ekleyip RF>1 yap) ----
    @Bean
    public ConsistentHashRing<Node<String,String>> ring(
            @Value("${app.cluster.virtualNodes:64}") int vnodes
    ) {
        HashFn hash = key -> java.util.Arrays.hashCode(key); // basit demo hash
        return new ConsistentHashRing<>(hash, vnodes);
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
            Node<String,String> localNode,
            @Value("${app.cluster.replicationFactor:1}") int rf
    ) {
        ring.addNode(localNode, localNode.id().getBytes());
        return new ClusterClient<>(ring, rf, StringCodec.UTF8);
    }
}