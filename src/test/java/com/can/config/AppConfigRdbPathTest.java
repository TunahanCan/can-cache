package com.can.config;

import com.can.core.CacheEngine;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.QuarkusTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(AppConfigRdbPathTest.CustomSnapshotProfile.class)
class AppConfigRdbPathTest {

    @Inject
    CacheEngine<String, String> engine;

    @AfterAll
    static void cleanup() throws IOException {
        Files.deleteIfExists(CustomSnapshotProfile.SNAPSHOT_FILE);
    }

    @Nested
    class CustomSnapshotPath {
        /**
         * Konfigürasyonda verilen RDB yolu AppConfig tarafından SnapshotFile'e aktarılır.
         * Test, dosyada bulunan Base64 kodlu girdinin uygulama açılışında CacheEngine'e yüklendiğini doğrular.
         */
        @Test
        void replaysDataFromCustomPath() {
            assertEquals("bar", engine.get("foo"));
        }
    }

    static class CustomSnapshotProfile implements QuarkusTestProfile {

        static final Path SNAPSHOT_FILE;
        private static final String KEY = Base64.getEncoder().encodeToString("foo".getBytes(StandardCharsets.UTF_8));
        private static final String VALUE = Base64.getEncoder().encodeToString("bar".getBytes(StandardCharsets.UTF_8));

        public CustomSnapshotProfile() {
        }

        static {
            try {
                SNAPSHOT_FILE = Files.createTempFile("can-cache-test", ".rdb");
                Files.writeString(SNAPSHOT_FILE, "S " + KEY + " " + VALUE + " 0\n");
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize snapshot file", e);
            }
        }

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "app.rdb.path", SNAPSHOT_FILE.toString(),
                    "app.rdb.snapshot-interval-seconds", "3600"
            );
        }
    }
}
