package com.can.loadbalancer;

import com.can.loadbalancer.config.LoadBalancerConfig;
import io.quarkus.runtime.Startup;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP tabanlı yük dengeleyici bileşenidir. Gelen istemci bağlantılarını canlı
 * can-cache düğümlerine round-robin stratejisiyle iletir.
 */
@Startup
@Singleton
public class CanCacheLoadBalancer implements AutoCloseable
{
    private static final Logger LOG = Logger.getLogger(CanCacheLoadBalancer.class);

    private final Vertx vertx;
    private final ClusterMembershipView membershipView;
    private final LoadBalancerConfig.LoadBalancer config;
    private final boolean enabled;
    private final AtomicInteger rrCounter = new AtomicInteger();

    private NetServer netServer;
    private NetClient netClient;

    @Inject
    public CanCacheLoadBalancer(Vertx vertx,
                                ClusterMembershipView membershipView,
                                LoadBalancerConfig config)
    {
        this.vertx = Objects.requireNonNull(vertx, "vertx");
        this.membershipView = Objects.requireNonNull(membershipView, "membershipView");
        Objects.requireNonNull(config, "config");
        this.config = config.loadBalancer();
        this.enabled = this.config.enabled();
    }

    @PostConstruct
    void start()
    {
        if (!enabled) {
            LOG.info("can-cache-load-balancer devre dışı bırakıldı (app.load-balancer.enabled=false)");
            return;
        }

        NetServerOptions serverOptions = new NetServerOptions()
                .setHost(config.host())
                .setPort(config.port())
                .setTcpNoDelay(true)
                .setReuseAddress(true)
                .setAcceptBacklog(Math.max(1, config.backlog()));
        netServer = vertx.createNetServer(serverOptions);

        NetClientOptions clientOptions = new NetClientOptions()
                .setConnectTimeout(Math.max(100, config.connectTimeoutMillis()))
                .setTcpNoDelay(true)
                .setReuseAddress(true);
        netClient = vertx.createNetClient(clientOptions);

        netServer.connectHandler(this::handleClientConnection);

        try {
            netServer.listen().toCompletionStage().toCompletableFuture().join();
        } catch (RuntimeException e) {
            throw new IllegalStateException("Yük dengeleyici portu dinlenemedi", e);
        }

        LOG.infof("can-cache-load-balancer %s:%d adresinde dinlemede", config.host(), netServer.actualPort());
    }

    private void handleClientConnection(NetSocket clientSocket)
    {
        BackendEndpoint backend = selectBackend();
        if (backend == null) {
            LOG.warn("Aktif can-cache düğümü bulunamadı, bağlantı sonlandırılıyor");
            clientSocket.write("SERVER_ERROR no backend available\r\n").onComplete(v -> clientSocket.close());
            return;
        }

        netClient.connect(backend.port(), backend.host(), ar -> {
            if (ar.failed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debugf(ar.cause(), "Backend %s:%d bağlantısı kurulamadı", backend.host(), backend.port());
                }
                clientSocket.write("SERVER_ERROR backend unavailable\r\n").onComplete(v -> clientSocket.close());
                return;
            }

            NetSocket backendSocket = ar.result();

            backendSocket.handler(buffer -> clientSocket.write(buffer));
            backendSocket.exceptionHandler(e -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debugf(e, "Backend soket hatası %s:%d", backend.host(), backend.port());
                }
                backendSocket.close();
            });
            backendSocket.closeHandler(v -> clientSocket.close());

            clientSocket.handler(buffer -> backendSocket.write(buffer));
            clientSocket.exceptionHandler(e -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debugf(e, "İstemci soket hatası %s", clientSocket.remoteAddress());
                }
                clientSocket.close();
            });
            clientSocket.closeHandler(v -> backendSocket.close());
        });
    }

    private BackendEndpoint selectBackend()
    {
        List<BackendEndpoint> endpoints = membershipView.snapshot();
        if (endpoints.isEmpty()) {
            return null;
        }
        int index = Math.floorMod(rrCounter.getAndIncrement(), endpoints.size());
        return endpoints.get(index);
    }

    @PreDestroy
    @Override
    public void close()
    {
        if (!enabled) {
            return;
        }
        if (netServer != null) {
            netServer.close().toCompletionStage().toCompletableFuture().join();
        }
        if (netClient != null) {
            netClient.close().toCompletionStage().toCompletableFuture().join();
        }
    }
}
