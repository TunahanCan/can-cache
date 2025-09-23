package com.can.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Optional;

/**
 * Uygulama yapılandırma değerlerini tip güvenli bir şekilde okumak için kullanılan
 * konfigürasyon arayüzüdür. Alt arayüzler metrik raporlama sıklığı, RDB anlık
 * görüntü ayarları, önbellek segment sayısı ve kapasitesi ile küme topolojisini
 * belirleyen parametreler gibi alanları gruplayarak {@code application.properties}
 * içindeki "app" önekiyle başlayan değerleri CDI bileşenlerine sağlar.
 */

@ConfigMapping(prefix = "app")
public interface AppProperties
{

    Metrics metrics();
    Rdb rdb();
    Cache cache();
    Cluster cluster();
    Network network();
    Memcache memcache();

    interface Metrics {
        @WithDefault("5")
        long reportIntervalSeconds();
    }

    interface Rdb {
        @WithDefault("data.rdb")
        String path();

        @WithDefault("60")
        long snapshotIntervalSeconds();
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

    interface Network {
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
    }

    interface Coordination {
        @WithDefault("5000")
        long hintReplayIntervalMillis();

        @WithDefault("30000")
        long antiEntropyIntervalMillis();
    }

    interface Memcache {
        @WithDefault("1048576")
        int maxItemSizeBytes();

        @WithDefault("16")
        int maxCasRetries();
    }
}
