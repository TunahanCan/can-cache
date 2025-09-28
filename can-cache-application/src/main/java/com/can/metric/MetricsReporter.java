package com.can.metric;

import com.can.cluster.ClusterState;
import com.can.config.AppProperties;
import io.quarkus.runtime.Startup;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Prometheus uyumlu metrik uç noktası sağlayan yardımcı servistir. Uç nokta
 * varsayılan olarak {@code /metrics} yolu üzerinden HTTP ile yayınlanır ve
 * sayaç/zamanlayıcı değerlerini istemci talep ettiğinde üretir. Böylece log
 * kazımaya gerek kalmadan gösterge panolarına veri sağlanabilir.
 */
@Startup
@Singleton
public class MetricsReporter implements AutoCloseable
{
    private static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";
    private static final Logger LOG = Logger.getLogger(MetricsReporter.class);

    private final MetricsRegistry registry;
    private final Supplier<String> nodeIdSupplier;
    private final String replicationRole;
    private final String metricsPath;
    private final String listenHost;
    private final int listenPort;
    private final boolean enabled;
    private final Vertx vertx;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile HttpServer httpServer;
    private volatile int actualPort;

    @Inject
    public MetricsReporter(MetricsRegistry registry,
                           AppProperties properties,
                           ClusterState clusterState,
                           Vertx vertx)
    {
        this(registry,
                properties.metrics(),
                clusterState != null ? clusterState::localNodeId : () -> "unknown",
                vertx);
    }

    MetricsReporter(MetricsRegistry registry,
                    AppProperties.Metrics metricsConfig,
                    Supplier<String> nodeIdSupplier,
                    Vertx vertx)
    {
        this.registry = registry;
        this.nodeIdSupplier = nodeIdSupplier != null ? nodeIdSupplier : () -> "unknown";
        this.replicationRole = sanitizeLabelValue(metricsConfig.replicationRole());
        this.metricsPath = normalisePath(metricsConfig.endpointPath());
        this.listenHost = metricsConfig.endpointHost();
        this.listenPort = metricsConfig.endpointPort();
        this.enabled = metricsConfig.endpointEnabled();
        this.vertx = vertx;
    }

    @PostConstruct
    void init()
    {
        start();
    }

    public synchronized void start()
    {
        if (!enabled || running.get()) {
            return;
        }

        HttpServerOptions options = new HttpServerOptions()
                .setHost(listenHost)
                .setPort(listenPort);

        HttpServer server = vertx.createHttpServer(options)
                .requestHandler(this::handleRequest);

        try {
            server.listen().toCompletionStage().toCompletableFuture().join();
        }
        catch (RuntimeException e)
        {
            throw new IllegalStateException("Failed to start metrics endpoint", e);
        }

        this.httpServer = server;
        this.actualPort = server.actualPort();

        LOG.infof("Metrics endpoint started on %s:%d%s", listenHost, actualPort, metricsPath);
        running.set(true);
    }

    public boolean isRunning()
    {
        return running.get();
    }

    public int actualPort()
    {
        return actualPort;
    }

    private void handleRequest(HttpServerRequest request)
    {
        if (!running.get()) {
            request.response().setStatusCode(503).end();
            return;
        }

        if (!"GET".equals(request.method().name()) || !matchesPath(request.path())) {
            request.response().setStatusCode(404).end();
            return;
        }

        String body = renderMetrics();
        request.response()
                .putHeader("content-type", CONTENT_TYPE)
                .end(body);
    }

    private boolean matchesPath(String path)
    {
        if (path == null) {
            return false;
        }
        if (path.equals(metricsPath)) {
            return true;
        }
        if (path.endsWith("/")) {
            return matchesPath(path.substring(0, path.length() - 1));
        }
        return false;
    }

    private String renderMetrics()
    {
        StringBuilder sb = new StringBuilder(1024);
        Map<String, Counter> counters = new TreeMap<>(registry.counters());
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            String name = entry.getKey();
            Counter counter = entry.getValue();
            String prometheusName = formatCounterName(name);
            sb.append("# TYPE ").append(prometheusName).append(" counter\n");
            sb.append(prometheusName)
                    .append(formatLabels(additionalLabelsForCounter(name)))
                    .append(' ')
                    .append(counter.get())
                    .append('\n');
        }

        Map<String, Timer> timers = new TreeMap<>(registry.timers());
        for (Timer timer : timers.values()) {
            Timer.Sample sample = timer.snapshot();
            String prometheusName = formatTimerName(sample.name());
            sb.append("# TYPE ").append(prometheusName).append(" summary\n");
            sb.append(prometheusName)
                    .append(formatLabels(Map.of("quantile", "0.5")))
                    .append(' ')
                    .append(formatDouble(nsToSeconds(sample.p50Ns())))
                    .append('\n');
            sb.append(prometheusName)
                    .append(formatLabels(Map.of("quantile", "0.95")))
                    .append(' ')
                    .append(formatDouble(nsToSeconds(sample.p95Ns())))
                    .append('\n');
            sb.append(prometheusName).append("_count")
                    .append(formatLabels(Map.of()))
                    .append(' ')
                    .append(sample.count())
                    .append('\n');
            sb.append(prometheusName).append("_sum")
                    .append(formatLabels(Map.of()))
                    .append(' ')
                    .append(formatDouble(nsToSeconds(sample.totalNs())))
                    .append('\n');
        }

        return sb.toString();
    }

    private Map<String, String> additionalLabelsForCounter(String name)
    {
        if ("hinted_handoff_replayed_total".equals(name)) {
            return Map.of("hint_replay_result", "success");
        }
        if ("hinted_handoff_failures_total".equals(name)) {
            return Map.of("hint_replay_result", "failure");
        }
        return Map.of();
    }

    private String formatLabels(Map<String, String> extra)
    {
        Map<String, String> labels = new TreeMap<>();
        labels.put("node_id", sanitizeLabelValue(nodeIdSupplier.get()));
        labels.put("role", replicationRole);
        labels.putAll(extra);
        if (labels.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append(entry.getKey())
                    .append('=')
                    .append('"')
                    .append(escapeLabelValue(entry.getValue()))
                    .append('"');
            first = false;
        }
        sb.append('}');
        return sb.toString();
    }

    private static String formatCounterName(String name)
    {
        String sanitised = sanitiseMetricName(name);
        if (!sanitised.endsWith("_total")) {
            sanitised = sanitised + "_total";
        }
        return sanitised;
    }

    private static String formatTimerName(String name)
    {
        String sanitised = sanitiseMetricName(name);
        if (!sanitised.endsWith("_seconds")) {
            sanitised = sanitised + "_seconds";
        }
        return sanitised;
    }

    private static String sanitiseMetricName(String name)
    {
        if (name == null || name.isEmpty()) {
            return "metric";
        }
        StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (i == 0 && Character.isDigit(c)) {
                sb.append('_').append(c);
            }
            else if (!isValidMetricChar(c)) {
                sb.append('_');
            }
            else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static boolean isValidMetricChar(char c)
    {
        return (c >= 'a' && c <= 'z')
                || (c >= 'A' && c <= 'Z')
                || (c >= '0' && c <= '9')
                || c == ':'
                || c == '_';
    }

    private static double nsToSeconds(long nanos)
    {
        return nanos / 1_000_000_000.0;
    }

    private static String formatDouble(double value)
    {
        return String.format(Locale.ROOT, "%.9f", value);
    }

    private static String normalisePath(String path)
    {
        if (path == null || path.isBlank()) {
            return "/metrics";
        }
        String trimmed = path.trim();
        if (!trimmed.startsWith("/")) {
            trimmed = '/' + trimmed;
        }
        if (trimmed.length() > 1 && trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static String sanitizeLabelValue(String value)
    {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value.trim();
    }

    private static String escapeLabelValue(String value)
    {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' || c == '"' || c == '\n') {
                sb.append('\\');
                if (c == '\n') {
                    sb.append('n');
                    continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    @PreDestroy
    void shutdown()
    {
        close();
    }

    @Override
    public synchronized void close()
    {
        if (!running.get()) {
            return;
        }
        HttpServer server = this.httpServer;
        running.set(false);
        if (server != null) {
            server.close().toCompletionStage().toCompletableFuture().join();
        }
        httpServer = null;
    }
}
