package com.can.core;

import com.can.codec.StringCodec;
import com.can.metric.MetricsRegistry;
import com.can.metric.Timer;
import com.can.pubsub.Broker;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class CacheEngineTest
{
    private Vertx vertx;
    private CacheEngine<String, String> engine;
    private MetricsRegistry metrics;
    private RecordingBroker broker;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        metrics = new MetricsRegistry();
        broker = new RecordingBroker();
        engine = CacheEngine.<String, String>builder(StringCodec.UTF8, StringCodec.UTF8)
                .segments(2)
                .maxCapacity(16)
                .cleanerPollMillis(5)
                .metrics(metrics)
                .broker(broker)
                .vertx(vertx)
                .build();
    }

    @AfterEach
    void cleanup()
    {
        if (engine != null)
        {
            engine.close();
        }
        if (broker != null)
        {
            broker.close();
        }
        if (vertx != null)
        {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    @Nested
    class SetVeGetDavranisi
    {
        // Bu test set ve get çağrılarının değeri koruduğunu ve metriklerin güncellendiğini doğrular.
        @Test
        void set_ve_get_degeri_korur_ve_metrikleri_gunceller()
        {
            assertTrue(engine.set("key", "value"));
            assertEquals("value", engine.get("key"));
            assertEquals(1, engine.size());

            assertEquals(1L, metrics.counter("cache_hits").get());
            Timer.Sample setSample = metrics.timer("cache_set").snapshot();
            assertTrue(setSample.count() >= 1);
            Timer.Sample getSample = metrics.timer("cache_get").snapshot();
            assertTrue(getSample.count() >= 1);
            assertTrue(broker.events().contains("keyspace:set:key"));
        }

        // Bu test TTL dolduğunda get çağrısının girdiyi sildiğini gösterir.
        @Test
        void ttl_suresi_gecince_get_cagrisi_kaydi_siler()
        {
            assertTrue(engine.set("expire", "value", Duration.ofMillis(15)));
            sleep(40);

            assertNull(engine.get("expire"));
            assertFalse(engine.exists("expire"));
            assertTrue(metrics.counter("cache_misses").get() >= 1);
            assertTrue(broker.events().contains("keyspace:del:expire"));
        }

        // Bu test çok büyük TTL değerlerinin taşma oluşturmadan saklandığını kontrol eder.
        @Test
        void asiri_uzun_ttl_tasma_yapmadan_saklanir()
        {
            long now = System.currentTimeMillis();
            Duration ttl = Duration.ofMillis(Long.MAX_VALUE - now - 1);
            assertTrue(engine.set("forever", "value", ttl));
            assertEquals("value", engine.get("forever"));
            assertTrue(engine.exists("forever"));
        }
    }

    @Nested
    class CompareAndSwapDavranisi
    {
        // Bu test CAS beklentisi sağlandığında değerin ve TTL'nin güncellendiğini ispatlar.
        @Test
        void compare_and_swap_eslesen_cas_ile_degeri_gunceller()
        {
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue("v1".getBytes(StandardCharsets.UTF_8), 1, 9L, 0L);
            String encoded = StoredValueCodec.encode(base);
            assertTrue(engine.set("cas", encoded));

            StoredValueCodec.StoredValue updated = base.withValue("v2".getBytes(StandardCharsets.UTF_8), 11L);
            String next = StoredValueCodec.encode(updated);
            assertTrue(engine.compareAndSwap("cas", next, 9L, Duration.ofMillis(30)));

            assertEquals(next, engine.get("cas"));
            sleep(60);
            assertFalse(engine.exists("cas"));
            assertTrue(broker.events().stream().anyMatch(e -> e.startsWith("keyspace:set:cas")));
        }

        // Bu test CAS beklentisi tutmadığında değerin değişmediğini doğrular.
        @Test
        void compare_and_swap_cas_uyusmadiginda_basarisiz_olur()
        {
            StoredValueCodec.StoredValue base = new StoredValueCodec.StoredValue("v1".getBytes(StandardCharsets.UTF_8), 1, 7L, 0L);
            String encoded = StoredValueCodec.encode(base);
            assertTrue(engine.set("cas", encoded));

            assertFalse(engine.compareAndSwap("cas", "ignored", 5L, null));
            assertEquals(encoded, engine.get("cas"));
        }

        // Bu test süresi dolmuş girdide CAS denemesi yapıldığında kaydın temizlendiğini doğrular.
        @Test
        void compare_and_swap_suresi_bitmis_girdiyi_siler()
        {
            assertTrue(engine.set("stale", "plain", Duration.ofMillis(10)));
            sleep(30);

            assertFalse(engine.compareAndSwap("stale", "new", 0L, null));
            assertFalse(engine.exists("stale"));
            assertTrue(broker.events().stream().anyMatch(e -> e.startsWith("keyspace:del:stale")));
        }
    }

    @Nested
    class ReplayDavranisi
    {
        // Bu test kalıcı logdan gelen set kaydının belleğe geri yüklendiğini gösterir.
        @Test
        void replay_set_komutu_degeri_yeniden_yukler()
        {
            engine.replay(new byte[]{'S'}, StringCodec.UTF8.encode("key"), StringCodec.UTF8.encode("value"), 0L);
            assertEquals("value", engine.get("key"));
        }

        // Bu test süresi geçmiş bir replay girdisinin dikkate alınmadığını kontrol eder.
        @Test
        void replay_suresi_gecmis_kaydi_yoksayar()
        {
            engine.replay(new byte[]{'S'}, StringCodec.UTF8.encode("late"), StringCodec.UTF8.encode("value"), System.currentTimeMillis() - 1_000);
            assertNull(engine.get("late"));
        }

        // Bu test replay delete kaydının ilgili anahtarı kaldırdığını doğrular.
        @Test
        void replay_delete_komutu_kaydi_siler()
        {
            assertTrue(engine.set("gone", "value"));
            engine.replay(new byte[]{'D'}, StringCodec.UTF8.encode("gone"), new byte[0], 0L);
            assertNull(engine.get("gone"));
        }
    }

    @Nested
    class SilmeBildirimleri
    {
        // Bu test manuel silme yapıldığında dinleyicinin bilgilendirildiğini doğrular.
        @Test
        void manual_silme_dinleyiciye_haber_verir() throws Exception
        {
            List<String> removed = new ArrayList<>();
            AutoCloseable handle = engine.onRemoval(removed::add);
            assertTrue(engine.set("target", "value"));
            assertTrue(engine.delete("target"));
            assertEquals(List.of("target"), removed);
            handle.close();
        }

        // Bu test TTL dolduğunda dinleyiciye haber gönderildiğini ispatlar.
        @Test
        void ttl_bitirince_dinleyici_tetiklenir()
        {
            List<String> removed = new ArrayList<>();
            engine.onRemoval(removed::add);
            assertTrue(engine.set("ttl", "value", Duration.ofMillis(15)));
            sleep(60);
            assertTrue(removed.contains("ttl"));
        }
    }

    @Nested
    class IterasyonVeOzet
    {
        // Bu test forEach çağrısının yalnızca süresi geçmemiş kayıtları aktardığını doğrular.
        @Test
        void for_each_sadece_gecerli_kayitlari_doner()
        {
            assertTrue(engine.set("kal", "value"));
            assertTrue(engine.set("git", "value", Duration.ofMillis(10)));
            sleep(40);

            List<String> keys = new ArrayList<>();
            engine.forEachEntry((key, value, expireAt) -> keys.add(key));
            assertEquals(List.of("kal"), keys);
        }

        // Bu test clear işleminin tüm segmentleri boşalttığını gösterir.
        @Test
        void clear_butun_segmentleri_temizler()
        {
            assertTrue(engine.set("a", "1"));
            assertTrue(engine.set("b", "2"));
            engine.clear();
            assertEquals(0, engine.size());

            List<String> keys = new ArrayList<>();
            engine.forEachEntry((key, value, expireAt) -> keys.add(key));
            assertTrue(keys.isEmpty());
        }

        // Bu test fingerprint sonucunun ekleme sırası değişse bile sabit kaldığını doğrular.
        @Test
        void fingerprint_sirali_degisimde_sabit_kalir()
        {
            assertTrue(engine.set("one", "1"));
            assertTrue(engine.set("two", "2"));
            long first = engine.fingerprint();

            assertTrue(engine.delete("one"));
            assertTrue(engine.set("one", "1"));
            long second = engine.fingerprint();
            assertEquals(first, second);
        }
    }

    private static void sleep(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    private static final class RecordingBroker extends Broker
    {
        private final CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();

        @Override
        public void publish(String topic, byte[] payload)
        {
            String value = payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
            events.add(topic + ':' + value);
        }

        @Override
        public void close()
        {
            events.clear();
        }

        List<String> events()
        {
            return events;
        }
    }
}
