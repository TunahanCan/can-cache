package com.can.pubsub;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pub/Sub alt sistemi cache motorundaki olayları dinlemeyi sağlar, StreamLog ise basit bir kalıcı kayıt kuyruğudur.
 * Testler asenkron yayın, abonelikten çıkma ve log'un dairesel davranışını açıklar.
 */
class BrokerAndStreamLogTest {

    @Nested
    class BrokerBehaviour {
        /**
         * Broker her publish çağrısında kayıtlı aboneleri sanal thread üzerinde çalıştırır.
         * CountDownLatch ile asenkron çağrının gerçekleştiğini doğrular, AutoCloseable ile abonelikten çıkmayı deneriz.
         */
        @Test
        void deliversPayloadsToSubscribers() throws Exception {
            try (Broker broker = new Broker()) {
                CountDownLatch latch = new CountDownLatch(1);
                AutoCloseable subscription = broker.subscribe("topic", payload -> {
                    assertArrayEquals("selam".getBytes(StandardCharsets.UTF_8), payload);
                    latch.countDown();
                });

                broker.publish("topic", "selam".getBytes(StandardCharsets.UTF_8));
                assertTrue(latch.await(1, TimeUnit.SECONDS), "Asenkron mesaj abonelere ulaşmalı");

                subscription.close();
                CountDownLatch second = new CountDownLatch(1);
                AutoCloseable secondSub = broker.subscribe("topic", payload -> second.countDown());
                broker.publish("topic", new byte[0]);
                assertTrue(second.await(1, TimeUnit.SECONDS), "Yeni abonelik çalışmalı");
                secondSub.close();
            }
        }
    }

    @Nested
    class StreamLogBehaviour {
        /**
         * StreamLog maksimum kapasiteye ulaştığında en eski kayıtları silip yenilerine yer açar.
         * xrange metodu FIFO sırasını koruyarak istenen sayıda kayıt döndürür.
         */
        @Test
        void keepsSlidingWindowOfEntries() {
            StreamLog log = new StreamLog(2);
            log.xadd("bir".getBytes(StandardCharsets.UTF_8));
            log.xadd("iki".getBytes(StandardCharsets.UTF_8));
            log.xadd("uc".getBytes(StandardCharsets.UTF_8));

            List<byte[]> lastTwo = log.xrange(5);
            assertEquals(2, lastTwo.size());
            assertArrayEquals("iki".getBytes(StandardCharsets.UTF_8), lastTwo.get(0));
            assertArrayEquals("uc".getBytes(StandardCharsets.UTF_8), lastTwo.get(1));
        }
    }
}
