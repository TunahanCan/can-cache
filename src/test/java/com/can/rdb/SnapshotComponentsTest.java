package com.can.rdb;

import com.can.codec.StringCodec;
import com.can.core.CacheEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SnapshotFile ve SnapshotScheduler cache durumunu diske yazarak kalıcılık sağlar.
 * Testler yazma/okuma akışını ve zamanlanmış snapshot davranışını ayrıntılı biçimde açıklar.
 */
class SnapshotComponentsTest {

    private CacheEngine<String, String> engine;
    private SnapshotScheduler scheduler;

    @AfterEach
    void tearDown() throws Exception {
        if (scheduler != null) {
            scheduler.close();
            scheduler = null;
        }
        if (engine != null) {
            engine.close();
            engine = null;
        }
    }

    @Nested
    class SnapshotFileBehaviour {
        /**
         * write metodu CacheEngine.forEachEntry üzerinden Base64 kodlu kayıtları temp dosyaya yazar.
         * load çağrısı geçerli satırları okuyup CacheEngine.replay ile belleğe geri yükler, bozuk satırlar atlanır.
         */
        @Test
        void writesAndLoadsCacheEntries() throws Exception {
            File temp = File.createTempFile("can-cache", ".rdb");
            SnapshotFile<String, String> snapshot = new SnapshotFile<>(temp, StringCodec.UTF8);

            engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(8)
                    .build();
            engine.set("alive", "1");
            engine.set("ttl", "2", Duration.ofSeconds(1));
            snapshot.write(engine);
            engine.close();

            CacheEngine<String, String> empty = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(8)
                    .build();

            Files.writeString(temp.toPath(), Files.readString(temp.toPath()) + "\nM bozuk satir\n");

            snapshot.load(empty);
            assertEquals("1", empty.get("alive"));
            assertEquals("2", empty.get("ttl"));
            empty.close();
        }
    }

    @Nested
    class SnapshotSchedulerBehaviour {
        /**
         * Scheduler başlatıldığında önce hemen bir snapshot yazar, ardından belirlenen aralıklarla tekrarlar.
         * Testte aralık 0 vererek yalnızca başlangıç snapshot'unun oluştuğunu doğrularız.
         */
        @Test
        void triggersImmediateSnapshotOnStart() throws Exception {
            File temp = File.createTempFile("can-cache", ".rdb");
            SnapshotFile<String, String> snapshot = new SnapshotFile<>(temp, StringCodec.UTF8);
            engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(4)
                    .build();
            engine.set("key", "value");

            scheduler = new SnapshotScheduler(engine, snapshot, 0);
            scheduler.start();

            Thread.sleep(200);

            assertTrue(Files.size(temp.toPath()) > 0, "Başlangıç snapshot dosyaya yazılmalı");
        }
    }
}
