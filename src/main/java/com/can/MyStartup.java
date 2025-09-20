package com.can;

import com.can.cluster.ClusterClient;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
class MyStartup implements ApplicationRunner {
    private final ClusterClient<String,String> cluster;

    MyStartup(ClusterClient<String,String> cluster){ this.cluster = cluster; }

    @Override public void run(ApplicationArguments args) {
        cluster.set("user:42","zeynep", java.time.Duration.ofMinutes(5));
        System.out.println(cluster.get("user:42"));
    }
}