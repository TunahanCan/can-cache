package com.can;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Uygulamanın Docker içinde ayağa kalktığında memcached metin protokolüne
 * cevap verdiğini doğrulayan entegrasyon testi.
 */
class CanCacheDockerIT {

    private static final Path QUARKUS_APP_DIR = Path.of("target", "quarkus-app");
    private static GenericContainer<?> appContainer;

    @BeforeAll
    static void startApplicationContainer() {
        Assumptions.assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                "Docker runtime is required for integration tests");
        Assumptions.assumeTrue(Files.exists(QUARKUS_APP_DIR.resolve("quarkus-run.jar")),
                "Quarkus application must be packaged before running integration tests");

        appContainer = new GenericContainer<>("eclipse-temurin:25-jre")
                .withCopyFileToContainer(MountableFile.forHostPath(QUARKUS_APP_DIR.toAbsolutePath()), "/opt/can-cache")
                .withWorkingDirectory("/opt/can-cache")
                .withCommand("java", "-jar", "quarkus-run.jar")
                .withExposedPorts(11211)
                .waitingFor(Wait.forLogMessage(".*Memcached-compatible server listening.*", 1))
                .withStartupTimeout(Duration.ofSeconds(60));
        appContainer.start();
    }

    @AfterAll
    static void stopApplicationContainer() {
        if (appContainer != null) {
            appContainer.stop();
        }
    }

    @Test
    void shouldStoreRetrieveAndDeleteValueThroughMemcachedProtocol() throws Exception {
        Assumptions.assumeTrue(appContainer != null && appContainer.isRunning(),
                "Application container did not start");

        try (Socket socket = new Socket(appContainer.getHost(), appContainer.getMappedPort(11211));
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.US_ASCII))) {

            socket.setSoTimeout(5000);

            String cacheKey = "integration-key";
            String cacheValue = "docker-test";

            sendCommand(writer, "set " + cacheKey + " 0 60 " + cacheValue.length(), cacheValue);
            assertEquals("STORED", reader.readLine(), "Value should be stored successfully");

            sendCommand(writer, "get " + cacheKey, null);
            String header = reader.readLine();
            assertNotNull(header, "Memcached response must not be null");
            String[] headerParts = header.split(" ");
            assertEquals(4, headerParts.length, "Memcached VALUE header should have four parts");
            assertEquals("VALUE", headerParts[0]);
            assertEquals(cacheKey, headerParts[1]);
            assertEquals("0", headerParts[2]);
            assertEquals(String.valueOf(cacheValue.length()), headerParts[3]);
            assertEquals(cacheValue, reader.readLine(), "Returned payload should match stored value");
            assertEquals("END", reader.readLine(), "Memcached get should terminate with END");

            sendCommand(writer, "delete " + cacheKey, null);
            assertEquals("DELETED", reader.readLine(), "Delete operation should report success");

            sendCommand(writer, "get " + cacheKey, null);
            String endLine = reader.readLine();
            assertNotNull(endLine, "Response after delete should not be null");
            assertEquals("END", endLine, "Deleted key should not be returned again");
        }
    }

    private static void sendCommand(BufferedWriter writer, String command, String payload) throws IOException {
        writer.write(command);
        writer.write("\r\n");
        if (payload != null) {
            writer.write(payload);
            writer.write("\r\n");
        }
        writer.flush();
    }
}
