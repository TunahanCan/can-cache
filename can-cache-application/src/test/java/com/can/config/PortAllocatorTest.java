package com.can.config;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PortAllocatorTest
{
    @Test
    void assignsNextPortWhenConfiguredPortBusy() throws Exception
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            int busyPort = socket.getLocalPort();
            FakeAppProperties properties = new FakeAppProperties(busyPort);

            PortAllocator allocator = new PortAllocator(properties);

            assertTrue(allocator.networkPort() > busyPort,
                    () -> "Expected allocated port > " + busyPort + " but was " + allocator.networkPort());
        }
    }

    @Test
    void assignsNextPortWhenLoopbackPortBusyAndHostWildcard() throws Exception
    {
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        try (ServerSocket socket = new ServerSocket(0, 50, loopback)) {
            socket.setReuseAddress(true);
            int busyPort = socket.getLocalPort();
            FakeAppProperties properties = new FakeAppProperties(busyPort);

            PortAllocator allocator = new PortAllocator(properties);

            assertTrue(allocator.networkPort() > busyPort,
                    () -> "Expected allocated port > " + busyPort + " but was " + allocator.networkPort());
        }
    }

    private static final class FakeAppProperties implements AppProperties
    {
        private final int networkPort;

        private FakeAppProperties(int networkPort)
        {
            this.networkPort = networkPort;
        }

        @Override
        public Metrics metrics()
        {
            return new FakeMetrics();
        }

        @Override
        public Rdb rdb()
        {
            return new FakeRdb();
        }

        @Override
        public Cache cache()
        {
            return new FakeCache();
        }

        @Override
        public Cluster cluster()
        {
            return new FakeCluster();
        }

        @Override
        public Network network()
        {
            return new FakeNetwork(networkPort);
        }

        @Override
        public Cancache cancache()
        {
            return new FakeCancache();
        }
    }

    private static final class FakeMetrics implements AppProperties.Metrics
    {
        @Override
        public long reportIntervalSeconds()
        {
            return 5;
        }

        @Override
        public boolean endpointEnabled()
        {
            return false;
        }

        @Override
        public String endpointHost()
        {
            return "";
        }

        @Override
        public int endpointPort()
        {
            return 0;
        }

        @Override
        public String endpointPath()
        {
            return "/metrics";
        }

        @Override
        public String replicationRole()
        {
            return "coordinator";
        }
    }

    private static final class FakeRdb implements AppProperties.Rdb
    {
        @Override
        public String path()
        {
            return "data.rdb";
        }

        @Override
        public long snapshotIntervalSeconds()
        {
            return 60;
        }
    }

    private static final class FakeCache implements AppProperties.Cache
    {
        @Override
        public int segments()
        {
            return 8;
        }

        @Override
        public int maxCapacity()
        {
            return 1000;
        }

        @Override
        public long cleanerPollMillis()
        {
            return 100;
        }

        @Override
        public String evictionPolicy()
        {
            return "LRU";
        }
    }

    private static final class FakeCluster implements AppProperties.Cluster
    {
        @Override
        public int virtualNodes()
        {
            return 32;
        }

        @Override
        public int replicationFactor()
        {
            return 1;
        }

        @Override
        public Discovery discovery()
        {
            return new FakeDiscovery();
        }

        @Override
        public Replication replication()
        {
            return new FakeReplication();
        }

        @Override
        public Coordination coordination()
        {
            return new FakeCoordination();
        }
    }

    private static final class FakeDiscovery implements AppProperties.Discovery
    {
        @Override
        public String multicastGroup()
        {
            return "230.0.0.1";
        }

        @Override
        public int multicastPort()
        {
            return 45565;
        }

        @Override
        public long heartbeatIntervalMillis()
        {
            return 5000;
        }

        @Override
        public long failureTimeoutMillis()
        {
            return 15000;
        }

        @Override
        public Optional<String> nodeId()
        {
            return Optional.empty();
        }
    }

    private static final class FakeReplication implements AppProperties.Replication
    {
        @Override
        public String bindHost()
        {
            return "0.0.0.0";
        }

        @Override
        public String advertiseHost()
        {
            return "127.0.0.1";
        }

        @Override
        public int port()
        {
            return 18080;
        }

        @Override
        public int connectTimeoutMillis()
        {
            return 5000;
        }
    }

    private static final class FakeCoordination implements AppProperties.Coordination
    {
        @Override
        public long hintReplayIntervalMillis()
        {
            return 5000;
        }

        @Override
        public long antiEntropyIntervalMillis()
        {
            return 30000;
        }
    }

    private static final class FakeNetwork implements AppProperties.Network
    {
        private final int port;

        private FakeNetwork(int port)
        {
            this.port = port;
        }

        @Override
        public String host()
        {
            return "";
        }

        @Override
        public int port()
        {
            return port;
        }

        @Override
        public int backlog()
        {
            return 128;
        }

        @Override
        public int eventLoopThreads()
        {
            return 0;
        }

        @Override
        public int workerThreads()
        {
            return 4;
        }
    }

    private static final class FakeCancache implements AppProperties.Cancache
    {
        @Override
        public int maxItemSizeBytes()
        {
            return 1024;
        }

        @Override
        public int maxCasRetries()
        {
            return 16;
        }
    }
}
