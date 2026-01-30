package com.can.metric;

import com.can.cluster.ClusterState;
import com.can.config.AppProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Micrometer tabanlı metrik yapılandırma servisidir.
 * <p>
 * Quarkus Micrometer extension'ı otomatik olarak {@code /q/metrics} veya
 * yapılandırılmış path'te Prometheus uyumlu endpoint sağlar.
 * Bu sınıf özel etiketler (node_id, role vb.) eklemek için kullanılır.
 * </p>
 * <p>
 * HTTP endpoint Quarkus tarafından otomatik yönetilir, manuel server oluşturmaya gerek yoktur.
 * </p>
 *
 * @see io.micrometer.core.instrument.MeterRegistry
 */
@Startup
@ApplicationScoped
public class MetricsReporter implements AutoCloseable
{
    private static final Logger LOG = Logger.getLogger(MetricsReporter.class);

    private final MeterRegistry meterRegistry;
    private final String nodeId;
    private final String replicationRole;

    @Inject
    public MetricsReporter(MeterRegistry meterRegistry,
                           AppProperties properties,
                           ClusterState clusterState)
    {
        this.meterRegistry = meterRegistry;
        this.nodeId = clusterState != null ? clusterState.localNodeId() : "unknown";
        this.replicationRole = properties.metrics().replicationRole();
    }

    /**
     * Test constructor.
     */
    MetricsReporter()
    {
        this.meterRegistry = null;
        this.nodeId = "test";
        this.replicationRole = "standalone";
    }

    @PostConstruct
    void init()
    {
        if (meterRegistry == null) {
            return;
        }

        // Ortak etiketleri MeterRegistry'ye ekle
        List<Tag> commonTags = List.of(
                Tag.of("node_id", sanitize(nodeId)),
                Tag.of("role", sanitize(replicationRole))
        );

        meterRegistry.config().commonTags(commonTags);

        LOG.infof("Micrometer metrics configured with node_id=%s, role=%s", nodeId, replicationRole);
        LOG.info("Prometheus endpoint available at /q/metrics (or configured path)");
    }

    private static String sanitize(String value)
    {
        return value == null || value.isBlank() ? "unknown" : value.trim();
    }

    public boolean isRunning()
    {
        return meterRegistry != null;
    }

    /**
     * Metrik endpoint portu.
     * Quarkus Micrometer kullanıldığı için bu metot -1 döner.
     * Gerçek port Quarkus HTTP yapılandırmasından alınır.
     *
     * @return her zaman -1 (Quarkus tarafından yönetilir)
     */
    public int actualPort()
    {
        return -1; // Quarkus tarafından yönetiliyor
    }

    @Override
    public void close()
    {
        // Micrometer lifecycle Quarkus tarafından yönetilir
        LOG.debug("MetricsReporter closed");
    }
}
