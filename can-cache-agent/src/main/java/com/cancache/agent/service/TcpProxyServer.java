package com.cancache.agent.service;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.model.ConnectionContext;
import com.cancache.agent.model.ConnectionRecord;
import com.cancache.agent.model.NodeStats;
import com.cancache.agent.model.UpstreamAddress;
import io.quarkus.runtime.Startup;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
@Startup
public class TcpProxyServer {

    private static final Logger LOG = Logger.getLogger(TcpProxyServer.class);

    @Inject
    Vertx vertx;

    @Inject
    AgentConfig config;

    @Inject
    UpstreamRegistry registry;

    @Inject
    UpstreamSelector selector;

    @Inject
    MetricsModel metrics;

    @Inject
    ConnectionTracker tracker;

    private final Set<NetSocket> active = ConcurrentHashMap.newKeySet();
    private NetServer server;
    private NetClient client;

    @PostConstruct
    void start() {
        client = vertx.createNetClient(new NetClientOptions()
                .setConnectTimeout(3000)
                .setTcpNoDelay(true)
                .setReuseAddress(true));
        server = vertx.createNetServer(new NetServerOptions()
                .setHost(config.listen().host())
                .setPort(config.listen().port())
                .setTcpNoDelay(true)
                .setReuseAddress(true));
        server.connectHandler(this::handleClient)
                .listen()
                .onSuccess(ok -> LOG.infov("proxy listening on {0}:{1}", config.listen().host(), config.listen().port()))
                .onFailure(err -> LOG.error("proxy start failed", err));
    }

    private void handleClient(NetSocket downstream) {
        active.add(downstream);
        downstream.pause();
        selector.select(registry.ready()).ifPresentOrElse(node -> connectUpstream(downstream, node),
                () -> {
                    metrics.addEvent("[ERR ] no ready upstream for client=" + downstream.remoteAddress());
                    downstream.close();
                    active.remove(downstream);
                });
    }

    private void connectUpstream(NetSocket downstream, NodeStats node) {
        UpstreamAddress address = node.upstreamAddress();
        String clientAddr = downstream.remoteAddress().toString();
        client.connect(address.port(), address.host())
                .onSuccess(upstream -> {
                    ConnectionContext ctx = new ConnectionContext(clientAddr, node.address());
                    node.incActiveConn();
                    metrics.incActiveConnections();
                    metrics.addEvent("[CONN] client=" + clientAddr + " -> upstream=" + node.address());
                    setupForwarding(downstream, upstream, node, ctx);
                })
                .onFailure(err -> {
                    node.incError();
                    metrics.addEvent("[ERR ] dial upstream failed=" + node.address() + " cause=" + err.getMessage());
                    downstream.close();
                    active.remove(downstream);
                });
    }

    private void setupForwarding(NetSocket downstream, NetSocket upstream, NodeStats node, ConnectionContext ctx) {
        downstream.setWriteQueueMaxSize(64 * 1024);
        upstream.setWriteQueueMaxSize(64 * 1024);

        final long idleMs = config.timeouts().idle().toMillis();
        final AtomicLong idleTimer = new AtomicLong(-1L);
        final AtomicBoolean closed = new AtomicBoolean(false);

        Runnable bumpIdle = () -> {
            long prev = idleTimer.getAndSet(-1);
            if (prev != -1) {
                vertx.cancelTimer(prev);
            }
            long nextId = vertx.setTimer(idleMs, t -> {
                metrics.addEvent("[ERR ] idle timeout client=" + ctx.clientAddr() + " upstream=" + ctx.upstreamAddr());
                downstream.close();
                upstream.close();
            });
            idleTimer.set(nextId);
        };
        bumpIdle.run();

        downstream.handler(buf -> {
            bumpIdle.run();
            forward(buf, upstream, downstream, true, ctx, node);
        });
        upstream.handler(buf -> {
            bumpIdle.run();
            forward(buf, downstream, upstream, false, ctx, node);
        });

        downstream.closeHandler(v -> closePair(downstream, upstream, node, ctx, idleTimer, closed));
        upstream.closeHandler(v -> closePair(downstream, upstream, node, ctx, idleTimer, closed));

        downstream.exceptionHandler(err -> {
            node.incError();
            metrics.addEvent("[ERR ] downstream io=" + err.getMessage());
            downstream.close();
        });
        upstream.exceptionHandler(err -> {
            node.incError();
            metrics.addEvent("[ERR ] upstream io=" + err.getMessage());
            upstream.close();
        });

        downstream.resume();
    }

    private void forward(Buffer buffer, NetSocket target, NetSocket source, boolean downstreamToUpstream, ConnectionContext ctx,
                         NodeStats node) {
        target.write(buffer, ar -> {
            if (ar.failed()) {
                node.incError();
                metrics.addEvent("[ERR ] proxy write failed=" + ar.cause().getMessage());
                source.close();
                target.close();
            }
        });
        if (target.writeQueueFull()) {
            source.pause();
            target.drainHandler(v -> {
                target.drainHandler(null);
                source.resume();
            });
        }
        long len = buffer.length();

        if (downstreamToUpstream) {
            ctx.addBytesIn(len);
            node.addBytesIn(len);
            metrics.addBytesIn(len);
        } else {
            ctx.addBytesOut(len);
            node.addBytesOut(len);
            metrics.addBytesOut(len);
        }
    }

    private void closePair(NetSocket downstream, NetSocket upstream, NodeStats node, ConnectionContext ctx, AtomicLong idleTimer,
                           AtomicBoolean closed) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        long timer = idleTimer.getAndSet(-1);
        if (timer != -1) {
            vertx.cancelTimer(timer);
        }

        active.remove(downstream);
        node.decActiveConn();
        metrics.decActiveConnections();

        downstream.close();
        upstream.close();

        tracker.add(new ConnectionRecord(ctx.startTime(), Instant.now(), ctx.clientAddr(), ctx.upstreamAddr(), ctx.bytesIn(),
                ctx.bytesOut()));
    }

    @PreDestroy
    void stop() {
        if (server != null) {
            server.close();
        }
        active.forEach(NetSocket::close);
        if (client != null) {
            client.close();
        }
    }
}
