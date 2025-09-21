package com.can.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "app")
public interface AppProperties {

    Metrics metrics();
    Aof aof();
    Cache cache();
    Cluster cluster();

    interface Metrics {
        @WithDefault("5")
        long reportIntervalSeconds();
    }

    interface Aof {
        @WithDefault("data.aof")
        String path();

        @WithDefault("true")
        boolean fsyncEvery();
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
