package com.can.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "app")
public interface AppProperties
{

    Metrics metrics();
    Rdb rdb();
    Cache cache();
    Cluster cluster();

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
    }
}
