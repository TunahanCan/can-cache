package com.can.core;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ExpiringKeyTest
{
    @Test
    void get_delay_converts_milliseconds_to_requested_unit()
    {
        long target = System.currentTimeMillis() + 200;
        ExpiringKey key = new ExpiringKey("k", 0, target);

        long delayMillis = key.getDelay(TimeUnit.MILLISECONDS);
        long delaySeconds = key.getDelay(TimeUnit.SECONDS);

        assertTrue(delayMillis <= 200 && delayMillis >= 0);
        assertTrue(delaySeconds <= 1);
    }

    @Test
    void compare_to_orders_by_expiration()
    {
        long now = System.currentTimeMillis();
        ExpiringKey sooner = new ExpiringKey("a", 0, now + 50);
        ExpiringKey later = new ExpiringKey("b", 0, now + 100);

        assertTrue(sooner.compareTo(later) < 0);
        assertTrue(later.compareTo(sooner) > 0);
        assertEquals(0, sooner.compareTo(new ExpiringKey("c", 0, sooner.expireAtMillis())));
    }
}
