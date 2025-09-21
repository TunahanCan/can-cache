package com.can;

import com.can.cluster.ClusterClient;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Singleton;

@Singleton
class MyStartup {
    private final ClusterClient<String, String> cluster;

    MyStartup(ClusterClient<String, String> cluster) {
        this.cluster = cluster;
    }

    void onStart(@Observes StartupEvent event) {
        cluster.set("user:42", "zeynep", java.time.Duration.ofMinutes(5));
        System.out.println(cluster.get("user:42"));
    }
}
