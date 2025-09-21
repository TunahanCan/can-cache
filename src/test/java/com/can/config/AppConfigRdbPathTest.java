package com.can.config;

import com.can.core.CacheEngine;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AppConfigRdbPathTest {

    @Nested
    class CustomSnapshotPath {
        /**
         * Konfigürasyonda verilen RDB yolu AppConfig tarafından SnapshotFile'e aktarılır.
         * Test, dosyada bulunan Base64 kodlu girdinin uygulama açılışında CacheEngine'e yüklendiğini doğrular.
         */
        @Test
        void replaysDataFromCustomPath() throws IOException {
            Path snapshotFile = Files.createTempFile("can-cache-test", ".rdb");
            String key = Base64.getEncoder().encodeToString("foo".getBytes(StandardCharsets.UTF_8));
            String value = Base64.getEncoder().encodeToString("bar".getBytes(StandardCharsets.UTF_8));
            Files.writeString(snapshotFile, "S " + key + " " + value + " 0\n");

            try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
                TestPropertyValues.of(
                        "app.rdb.path=" + snapshotFile,
                        "app.rdb.snapshot-interval-seconds=3600"
                ).applyTo(ctx);
                ctx.register(AppConfig.class);
                ctx.refresh();

                CacheEngine<String, String> engine = ctx.getBean(CacheEngine.class);
                assertEquals("bar", engine.get("foo"));
            }
        }
    }
}
