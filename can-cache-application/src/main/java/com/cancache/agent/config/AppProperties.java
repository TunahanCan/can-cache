package com.cancache.agent.config;

import com.cancache.agent.cluster.QuorumPolicy;
import com.cancache.agent.cluster.ReadRepairMode;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

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
        ReadRepair readRepair();
    }

    interface Discovery {
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

        @WithDefault("4")
        int taskThreads();

        @WithDefault("256")
        int taskQueueCapacity();

        @WithDefault("1000")
        int antiEntropyMaxRepairsPerRun();

        @WithDefault("100")
        int antiEntropyRepairRatePerSecond();

        @WithDefault("0")
        int remoteNodePoolSize();

        @WithDefault("0")
        int remoteNodeRequestQueueCapacity();
    }

    interface ReadRepair
    {
        @WithDefault("true")
        boolean enabled();

        @WithDefault("FAST")
        ReadRepairMode mode();

        @WithDefault("true")
        boolean async();

        @WithDefault("DEGRADED")
        QuorumPolicy quorumPolicy();

        @WithDefault("4")
        int maxThreads();

        @WithDefault("1024")
        int queueCapacity();

        @WithDefault("500")
        int rateLimitPerSecond();
    }

    interface Cancache
    {
        @WithDefault("1048576")
        int maxItemSizeBytes();

        @WithDefault("16")
        int maxCasRetries();
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

}
