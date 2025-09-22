package com.can.metric;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sayaç, zamanlayıcı ve raporlama bileşenleri cache motorunun gözlemlenebilirliğini sağlar.
 * Testlerde hem içsel istatistiklerin hem de raporlamanın beklenen şekilde çalıştığını yorumlarla belgeliyoruz.
 */
class MetricsComponentsTest {

    private MetricsReporter reporter;
    private PrintStream originalOut;

    @AfterEach
    void cleanup() {
        if (reporter != null) {
            reporter.close();
            reporter = null;
        }
        if (originalOut != null) {
            System.setOut(originalOut);
            originalOut = null;
        }
    }

    @Nested
    class CounterBehaviour {
        /**
         * Counter atomik sayaç kullanarak artışları toplar.
         * inc ve add çağrılarının toplamı get ile okunabilmeli.
         */
        @Test
        void accumulatesIncrements() {
            Counter counter = new Counter("hits");
            counter.inc();
            counter.add(4);
            assertEquals(5, counter.get());
            assertEquals("hits", counter.name());
        }
    }

    @Nested
    class TimerBehaviour {
        /**
         * Timer kayıtları ring-buffer tarzı reservoir'e yazar, snapshot istatistik döndürür.
         * Ortalama, min, max ve percentil değerlerinin beklenen aralıkta olduğunu kontrol ederiz.
         */
        @Test
        void aggregatesSampleStatistics() {
            Timer timer = new Timer("latency", 128);
            timer.record(100);
            timer.record(200);
            timer.record(400);

            Timer.Sample sample = timer.snapshot();
            assertEquals("latency", sample.name());
            assertEquals(3, sample.count());
            assertEquals(700, sample.totalNs());
            assertEquals(100, sample.minNs());
            assertEquals(400, sample.maxNs());
            assertTrue(sample.avgNs() >= 200 && sample.avgNs() <= 300);
            assertTrue(sample.p95Ns() >= sample.p50Ns(), "p95 her zaman p50'den büyük ya da eşit olmalı");
        }
    }

    @Nested
    class RegistryAndReporterBehaviour {
        /**
         * MetricsRegistry sayaç/zamanlayıcı örneklerini tekil tutar.
         * MetricsReporter start edildiğinde belirli aralıklarla System.out'a rapor yazar; çıktıyı yakalayıp doğrularız.
         */
        @Test
        void reportsRegisteredMetricsPeriodically() throws InterruptedException {
            MetricsRegistry registry = new MetricsRegistry();
            registry.counter("c").add(2);
            registry.timer("t").record(1_000_000);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            originalOut = System.out;
            System.setOut(new PrintStream(baos));

            reporter = new MetricsReporter(registry, 1);
            reporter.start(1);

            Thread.sleep(1_200); // en az bir dump çağrısı gerçekleşsin

            String output = baos.toString();
            assertTrue(output.contains("=== METRICS ===="));
            assertTrue(output.contains("counter c = 2"));
            assertTrue(output.contains("timer t"));
        }
    }
}
