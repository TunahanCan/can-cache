package com.can.core;

import com.can.core.model.ExpiringKey;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ExpiringKeyTest
{
    @Nested
    class ZamanDavranisi
    {
        // Bu test gelecekteki zamanlar için bekleme süresinin pozitif olduğunu doğrular.
        @Test
        void get_delay_gelecek_zaman_icin_pozitif_doner()
        {
            long expireAt = System.currentTimeMillis() + 200L;
            ExpiringKey key = new ExpiringKey("k", 0, expireAt);
            long delay = key.getDelay(TimeUnit.MILLISECONDS);
            assertTrue(delay > 0 && delay <= 200L);
        }

        // Bu test compareTo'nun en erken süresi olan anahtarı önce sıraladığını gösterir.
        @Test
        void compare_to_sureye_gore_siralama_yapar()
        {
            long now = System.currentTimeMillis();
            ExpiringKey early = new ExpiringKey("a", 0, now + 10);
            ExpiringKey late = new ExpiringKey("b", 0, now + 50);
            assertTrue(early.compareTo(late) < 0);
            assertTrue(late.compareTo(early) > 0);
        }
    }
}
