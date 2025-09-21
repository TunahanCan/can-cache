package com.can.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private final Metrics metrics = new Metrics();
    private final Aof aof = new Aof();
    private final Cache cache = new Cache();
    private final Cluster cluster = new Cluster();

    public Metrics getMetrics() {
        return metrics;
    }

    public Aof getAof() {
        return aof;
    }

    public Cache getCache() {
        return cache;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public static class Metrics {
        private long reportIntervalSeconds = 5L;

        public long getReportIntervalSeconds() {
            return reportIntervalSeconds;
        }

        public void setReportIntervalSeconds(long reportIntervalSeconds) {
            this.reportIntervalSeconds = reportIntervalSeconds;
        }
    }

    public static class Aof {
        private String path = "data.aof";
        private boolean fsyncEvery = true;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public boolean isFsyncEvery() {
            return fsyncEvery;
        }

        public void setFsyncEvery(boolean fsyncEvery) {
            this.fsyncEvery = fsyncEvery;
        }
    }

    public static class Cache {
        private int segments = 8;
        private int maxCapacity = 10_000;
        private long cleanerPollMillis = 100L;
        private String evictionPolicy = "LRU";

        public int getSegments() {
            return segments;
        }

        public void setSegments(int segments) {
            this.segments = segments;
        }

        public int getMaxCapacity() {
            return maxCapacity;
        }

        public void setMaxCapacity(int maxCapacity) {
            this.maxCapacity = maxCapacity;
        }

        public long getCleanerPollMillis() {
            return cleanerPollMillis;
        }

        public void setCleanerPollMillis(long cleanerPollMillis) {
            this.cleanerPollMillis = cleanerPollMillis;
        }

        public String getEvictionPolicy() {
            return evictionPolicy;
        }

        public void setEvictionPolicy(String evictionPolicy) {
            this.evictionPolicy = evictionPolicy;
        }
    }

    public static class Cluster {
        private int virtualNodes = 64;
        private int replicationFactor = 1;

        public int getVirtualNodes() {
            return virtualNodes;
        }

        public void setVirtualNodes(int virtualNodes) {
            this.virtualNodes = virtualNodes;
        }

        public int getReplicationFactor() {
            return replicationFactor;
        }

        public void setReplicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
        }
    }
}
