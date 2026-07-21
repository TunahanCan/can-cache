package com.cancache.agent.cache.perf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.junit.jupiter.api.Test;

class CancachedRoundTripSamplerTest {

    @Test
    void reusesOneConnectionForMultipleSamples() throws Exception {
        runScenario(1, 2);
    }

    @Test
    void reconnectsAfterTheServerClosesAConnection() throws Exception {
        runScenario(2, 1);
    }

    private static void runScenario(int expectedConnections, int samplesPerConnection) throws Exception {
        ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout((int) Duration.ofSeconds(5).toMillis());
            Future<Integer> serverResult = serverExecutor.submit(
                    () -> serve(server, expectedConnections, samplesPerConnection));

            CancachedRoundTripSampler sampler = new CancachedRoundTripSampler();
            JavaSamplerContext context = contextFor(server.getLocalPort());
            sampler.setupTest(context);
            try {
                int totalSamples = expectedConnections * samplesPerConnection;
                for (int i = 0; i < totalSamples; i++) {
                    SampleResult result = sampler.runTest(context);
                    assertTrue(result.isSuccessful(), result.getResponseMessage());
                }
            } finally {
                sampler.teardownTest(context);
            }

            assertEquals(expectedConnections, serverResult.get(5, TimeUnit.SECONDS));
        } finally {
            serverExecutor.shutdownNow();
            assertTrue(serverExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static JavaSamplerContext contextFor(int port) {
        Arguments arguments = new Arguments();
        arguments.addArgument("targetHost", "127.0.0.1");
        arguments.addArgument("targetPort", Integer.toString(port));
        arguments.addArgument("ttlSeconds", "60");
        arguments.addArgument("connectTimeoutMillis", "1000");
        arguments.addArgument("readTimeoutMillis", "1000");
        arguments.addArgument("keyPrefix", "test-");
        arguments.addArgument("payloadSize", "32");
        arguments.addArgument("payloadSizes", "");
        return new JavaSamplerContext(arguments);
    }

    private static int serve(ServerSocket server, int expectedConnections, int samplesPerConnection)
            throws Exception {
        int acceptedConnections = 0;
        for (int connection = 0; connection < expectedConnections; connection++) {
            try (Socket socket = server.accept();
                 BufferedReader reader = new BufferedReader(
                         new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                 BufferedWriter writer = new BufferedWriter(
                         new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
                acceptedConnections++;
                socket.setSoTimeout((int) Duration.ofSeconds(5).toMillis());
                for (int sample = 0; sample < samplesPerConnection; sample++) {
                    serveRoundTrip(reader, writer);
                }
            }
        }
        return acceptedConnections;
    }

    private static void serveRoundTrip(BufferedReader reader, BufferedWriter writer) throws Exception {
        String setCommand = reader.readLine();
        assertTrue(setCommand != null && setCommand.startsWith("set "), "Unexpected SET command: " + setCommand);
        String[] setParts = setCommand.split(" ");
        String key = setParts[1];
        String payload = reader.readLine();

        writer.write("STORED\r\n");
        writer.flush();

        assertEquals("get " + key, reader.readLine());
        writer.write("VALUE " + key + " 0 " + payload.getBytes(StandardCharsets.UTF_8).length + "\r\n");
        writer.write(payload);
        writer.write("\r\nEND\r\n");
        writer.flush();

        assertEquals("delete " + key, reader.readLine());
        writer.write("DELETED\r\n");
        writer.flush();
    }
}
