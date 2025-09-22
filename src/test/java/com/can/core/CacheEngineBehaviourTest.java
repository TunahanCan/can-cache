package com.can.core;

import com.can.codec.StringCodec;
import com.can.metric.MetricsRegistry;
import com.can.metric.Timer;
import com.can.pubsub.Broker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bu sınıf cache motorunun omurgasını test ederken her bir davranışın ardındaki tasarım amacını satır arası yorumlarda ayrıntılı anlatır.
 * CacheEngine veriyi segmentler üzerinde tutar, TTL kuyruğu ile siler, metrik ve pub/sub entegrasyonları sağlar.
 */
class CacheEngineBehaviourTest {

    private CacheEngine<String, String> engine;

    @AfterEach
    void tearDown() throws Exception {
        if (engine != null) {
            engine.close();
            engine = null;
        }
    }

    @Nested
    class BasicUsage {
        /**
         * CacheEngine#set anahtar-değer çiftlerini segmentlenmiş tablolarda saklar.
         * Her segment eşzamanlı erişim için kilitlenmiş LinkedHashMap kullanır, bu nedenle temel set/get akışını doğrularız.
         */
        @Test
        void storesAndRetrievesValues() {
            engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(2)
                    .maxCapacity(16)
                    .build();

            engine.set("k", "v");

            // get çağrısı veriyi codec ile çözerek döndürür; yoksa null verir.
            assertEquals("v", engine.get("k"));
            assertTrue(engine.exists("k"), "exists metodu TTL süresi dolmamış girdiler için true dönmeli");
            assertEquals(1, engine.size(), "size tüm segmentlerdeki toplam girdi sayısını verir");
        }

        /**
         * Silme işlemi segmentte kayıt olup olmadığına göre true/false döner ve TTL kuyruğundaki kayıtlar da temizlenmiş sayılır.
         */
        @Test
        void deleteRemovesEntryAndSignalsResult() {
            engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(4)
                    .build();

            engine.set("foo", "bar");
            assertTrue(engine.delete("foo"), "Var olan anahtar silindiğinde true dönmeli");
            assertFalse(engine.delete("foo"), "Yok olan anahtar silinirken false dönmeli");
            assertNull(engine.get("foo"));
        }
    }

    @Nested
    class TtlMechanics {
        /**
         * TTL verilen değerler ExpiringKey ile DelayQueue'ya yazılır, scheduled temizleyici poll ederek segmentten siler.
         * Küçük süreli TTL verip temizlenmesini bekleyerek mekanizmanın uçtan uca çalıştığını doğrularız.
         */
        @Test
        void removesEntriesAfterTtlExpires() throws Exception {
            engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(4)
                    .cleanerPollMillis(5)
                    .build();

            engine.set("temp", "data", Duration.ofMillis(50));
            assertEquals("data", engine.get("temp"));

            // Temizleyicinin çalışabilmesi için yeterince bekliyoruz.
            Thread.sleep(200);

            assertNull(engine.get("temp"), "Süre dolunca kayıt otomatik silinmeli");
            assertFalse(engine.exists("temp"), "exists de TTL dolduktan sonra false dönmeli");
        }

        /**
         * Expire olmuş bir kayıt get sırasında yakalanır ve delete çağrısıyla temizlenirken miss metriği artar.
         * Bu davranış "gecikmeli temizleme" stratejisini doğrular.
         */
        @Test
        void lazyGetCleansExpiredEntries() throws Exception {
            MetricsRegistry metrics = new MetricsRegistry();
            engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(2)
                    .cleanerPollMillis(5)
                    .metrics(metrics)
                    .build();

            engine.set("lazy", "value", Duration.ofMillis(10));
            Thread.sleep(100);

            assertNull(engine.get("lazy"), "Geçen sürede TTL dolduğu için get null dönmeli");
            assertEquals(1, metrics.counter("cache_misses").get(), "Expire olan kayıt miss olarak sayılmalı");
        }

        @Test
        void updatingTtlDoesNotRemoveNewValuePrematurely() throws Exception {
            engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(4)
                    .cleanerPollMillis(5)
                    .build();

            engine.set("ttl", "old", Duration.ofMillis(40));
            engine.set("ttl", "new", Duration.ofMillis(200));

            Thread.sleep(120);
            assertEquals("new", engine.get("ttl"), "Yeni TTL atanmış değer eski kuyruğa rağmen korunmalı");

            long deadline = System.currentTimeMillis() + 1_000;
            boolean removed = false;
            while (System.currentTimeMillis() < deadline) {
                if (engine.get("ttl") == null) {
                    removed = true;
                    break;
                }
                Thread.sleep(20);
            }

            assertTrue(removed, "Uzayan TTL süresi dolduğunda kayıt temizlenmeli");
            assertNull(engine.get("ttl"));
        }
    }

    @Nested
    class MetricsIntegration {
        /**
         * MetricsRegistry üzerinden alınan sayaç ve timer nesneleri CacheEngine içinde tutulur.
         * İşlemler yapıldığında sayaçlar artmalı, timer örnekleri kayıt sayısı tutmalı.
         */
        @Test
        void recordsCountersAndTimers() {
            MetricsRegistry metrics = new MetricsRegistry();
            engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(8)
                    .metrics(metrics)
                    .build();

            engine.set("m", "1");
            engine.set("m", "2");
            engine.get("m");
            engine.delete("missing");
            engine.delete("m");

            assertEquals(1, metrics.counter("cache_hits").get(), "Başarılı get çağrıları hit sayacını artırır");
            assertTrue(metrics.counter("cache_misses").get() >= 1, "Kaçırılan işlemler miss sayaçlarında görünmeli");

            Timer.Sample getSample = metrics.timer("cache_get").snapshot();
            assertTrue(getSample.count() >= 1, "get zamanlayıcısı en az bir ölçüm içermeli");
            assertTrue(metrics.timer("cache_set").snapshot().count() >= 1);
            assertTrue(metrics.timer("cache_del").snapshot().count() >= 1);
        }
    }

    @Nested
    class PubSubHooks {
        /**
         * Broker, keyspace olaylarını dinleyen abonelere publish eder.
         * CacheEngine set/delete çağrılarından sonra broker.publish tetiklenmeli.
         */
        @Test
        void notifiesBrokerOnMutations() throws Exception {
            try (Broker broker = new Broker()) {
                engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                        .segments(1)
                        .maxCapacity(4)
                        .broker(broker)
                        .build();

                CountDownLatch setLatch = new CountDownLatch(1);
                CountDownLatch delLatch = new CountDownLatch(1);

                try (AutoCloseable ignored1 = broker.subscribe("keyspace:set", payload -> setLatch.countDown());
                     AutoCloseable ignored2 = broker.subscribe("keyspace:del", payload -> delLatch.countDown())) {

                    engine.set("pub", "sub");
                    assertTrue(setLatch.await(1, TimeUnit.SECONDS), "Set sonrası publish bekleniyor");

                    engine.delete("pub");
                    assertTrue(delLatch.await(1, TimeUnit.SECONDS), "Delete sonrası publish bekleniyor");
                }
            }
        }
    }

    @Nested
    class ReplayAndIteration {
        /**
         * Snapshot dosyasından gelen S (set) kayıtları replay metoduna verildiğinde applyReplayEntry çağrılır.
         * Expire olmuş kayıtlar segmentten kaldırılır, temiz kayıtlar TTL kuyruğuna eklenir.
         */
        @Test
        void replaysEntriesAndSkipsExpiredOnes() throws Exception {
            engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(8)
                    .cleanerPollMillis(5)
                    .build();

            long future = System.currentTimeMillis() + 1_000;
            engine.replay(new byte[]{'S'}, StringCodec.UTF8.encode("a"), StringCodec.UTF8.encode("1"), future);

            long past = System.currentTimeMillis() - 1_000;
            engine.replay(new byte[]{'S'}, StringCodec.UTF8.encode("b"), StringCodec.UTF8.encode("2"), past);

            assertEquals("1", engine.get("a"));
            assertNull(engine.get("b"), "Geçmiş tarihli kayıt yüklenmemeli");
        }

        /**
         * forEachEntry TTL'i dolmamış kayıtları callback'e gönderir.
         * Bu yöntem snapshot alırken kullanılan veri aktarım kanalını temsil ettiği için sonuç listesini kontrol ederiz.
         */
        @Test
        void iteratesOnlyLiveEntries() {
            engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                    .segments(1)
                    .maxCapacity(8)
                    .build();

            engine.set("x", "1");
            engine.set("y", "2", Duration.ofDays(1));

            List<String> seen = new ArrayList<>();
            engine.forEachEntry((key, value, expireAt) -> seen.add(StringCodec.UTF8.decode(value)));

            assertEquals(List.of("1", "2"), seen);
        }
    }
}
