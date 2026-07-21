package com.cancache.agent.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Uygulama yapılandırma değerlerini tip güvenli bir şekilde okumak için kullanılan
 * konfigürasyon arayüzüdür. Alt arayüzler metrik raporlama sıklığı,
 * önbellek segment sayısı ve kapasitesi ile küme topolojisini
 * belirleyen parametreler gibi alanları gruplayarak {@code application.properties}
 * içindeki "app" önekiyle başlayan değerleri CDI bileşenlerine sağlar.
 */

@ConfigMapping(prefix = "app")
public interface AppProperties
{

    Metrics metrics();
    Cache cache();
    Cluster cluster();
    Network network();
    Cancache cancache();
    Agent agent();

    interface Metrics
    {
        @WithDefault("true")
        boolean endpointEnabled();

        @WithDefault("0.0.0.0")
        String endpointHost();

        @WithDefault("9000")
        int endpointPort();

        @WithDefault("/metrics")
        String endpointPath();

        @WithDefault("coordinator")
        String replicationRole();
    }

    interface Cache {
        @WithDefault("8")
        int segments();

        @WithDefault("10000")
        int maxCapacity();

        /**
         * Approximate upper bound for encoded payload bytes retained by the cache.
         * Entry metadata and JVM collection overhead are intentionally not included.
         */
        @WithDefault("268435456")
        long maxWeightBytes();

        @WithDefault("100")
        long cleanerPollMillis();

        @WithDefault("LRU")
        String evictionPolicy();
    }

    interface Cluster {
        @WithDefault("64")
        int virtualNodes();

        @WithDefault("1")
        int replicationFactor();

        Discovery discovery();
        Replication replication();
        Coordination coordination();
        Gossip gossip();
    }

    interface Discovery {
        @WithDefault("MULTICAST")
        DiscoveryType type();

        @WithDefault("230.0.0.1")
        String multicastGroup();

        @WithDefault("45565")
        int multicastPort();

        @WithDefault("5000")
        long heartbeatIntervalMillis();

        @WithDefault("15000")
        long failureTimeoutMillis();

        Optional<String> nodeId();
    }

    interface Gossip {
        @WithDefault("0.0.0.0")
        String bindHost();

        @WithDefault("45566") // Multicast portundan farklı bir port
        int port();

        @WithDefault("PT1S") // 1 saniyede bir ping
        Duration pingInterval();

        @WithDefault("PT0.5S") // 500ms'de bir dedikodu değişimi
        Duration gossipInterval();

        @WithDefault("PT10S") // 10 saniye yanıt yoksa şüpheli
        Duration failureTimeout();

        @WithDefault("PT30S") // 30 saniye sonra ölü üyeyi temizle
        Duration deadMemberCleanupDelay();

        @WithDefault("PT5S")
        Duration cleanupInterval();

        @WithDefault("localhost:45566") // Başlangıç tohum düğümleri
        List<String> seedNodes();
    }

    interface Replication {
        @WithDefault("0.0.0.0")
        String bindHost();

        @WithDefault("127.0.0.1")
        String advertiseHost();

        @WithDefault("18080")
        int port();

        @WithDefault("5000")
        int connectTimeoutMillis();
    }

    interface Network 
    {
        @WithDefault("0.0.0.0")
        String host();

        @WithDefault("11211")
        int port();

        @WithDefault("128")
        int backlog();

        @WithDefault("2048")
        int maxConnections();

        @WithDefault("300")
        int idleTimeoutSeconds();

        @WithDefault("0")
        int eventLoopThreads();

        @WithDefault("16")
        int workerThreads();

        @WithDefault("HELLO")
        String agreementPackMessage();

    }

    interface Coordination
    {
        @WithDefault("5000")
        long hintReplayIntervalMillis();

        @WithDefault("30000")
        long antiEntropyIntervalMillis();

        @WithDefault("10000")
        int maxHintsPerNode();

        @WithDefault("33554432")
        long maxHintBytesPerNode();
    }

    interface Cancache
    {
        @WithDefault("1048576")
        int maxItemSizeBytes();

        @WithDefault("16")
        int maxCasRetries();

        @WithDefault("128")
        int maxGetKeys();

        @WithDefault("16777216")
        int maxResponseSizeBytes();
    }

    interface Agent
    {
        @WithDefault("false")
        boolean enabled();

        @WithDefault("127.0.0.1")
        String host();

        @WithDefault("11211")
        int port();

        @WithDefault("11311")
        int registrationPort();

        @WithDefault("")
        String advertisedHost();

        @WithDefault("PT5S")
        java.time.Duration probeInterval();

        @WithDefault("PT1S")
        java.time.Duration connectTimeout();

        @WithDefault("PT0S")
        java.time.Duration startupWait();

        @WithDefault("false")
        boolean requiredOnStartup();
    }

    enum DiscoveryType {
        MULTICAST,
        DNS,
        GOSSIP
    }
}
