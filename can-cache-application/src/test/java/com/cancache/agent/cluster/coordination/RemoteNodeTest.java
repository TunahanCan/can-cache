package com.cancache.agent.cluster.coordination;

import io.vertx.core.Future;
import io.vertx.core.net.NetSocket;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class RemoteNodeTest
{
    @Test
    void shouldCloseConnectionThatCompletesAfterTimeout()
    {
        CompletableFuture<NetSocket> pending = new CompletableFuture<>();
        AtomicInteger closeCalls = new AtomicInteger();
        NetSocket socket = socketTrackingClose(closeCalls);

        pending.cancel(true);
        RemoteNode.completeConnection(pending, socket);

        assertEquals(1, closeCalls.get());
    }

    @Test
    void shouldKeepConnectionWhenConnectAttemptIsStillPending()
    {
        CompletableFuture<NetSocket> pending = new CompletableFuture<>();
        AtomicInteger closeCalls = new AtomicInteger();
        NetSocket socket = socketTrackingClose(closeCalls);

        RemoteNode.completeConnection(pending, socket);

        assertSame(socket, pending.join());
        assertEquals(0, closeCalls.get());
    }

    private static NetSocket socketTrackingClose(AtomicInteger closeCalls)
    {
        return (NetSocket) Proxy.newProxyInstance(
                RemoteNodeTest.class.getClassLoader(),
                new Class<?>[]{NetSocket.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close") && method.getParameterCount() == 0) {
                        closeCalls.incrementAndGet();
                        return Future.succeededFuture();
                    }
                    if (method.getName().equals("toString")) {
                        return "test-net-socket";
                    }
                    if (method.getName().equals("hashCode")) {
                        return System.identityHashCode(proxy);
                    }
                    if (method.getName().equals("equals")) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }
}
