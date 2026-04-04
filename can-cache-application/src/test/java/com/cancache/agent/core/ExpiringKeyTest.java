package com.cancache.agent.core;

import com.cancache.agent.core.model.ExpiringKey;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ExpiringKeyTest
{
    @Nested
    class TimingBehavior
    {
        /**
         * Verifies that the delay for future times is a positive value.
         */
        @Test
        void shouldReturnPositiveDelayForFutureTime()
        {
            // Given
            long expireAt = System.currentTimeMillis() + 200L;
            ExpiringKey key = new ExpiringKey("k", 0, expireAt);
            
            // When
            long delay = key.getDelay(TimeUnit.MILLISECONDS);
            
            // Then
            assertTrue(delay > 0 && delay <= 200L, "Delay should be positive and within the expected bound");
        }

        /**
         * Shows that compareTo orders keys by their expiration time, placing the earliest first.
         */
        @Test
        void shouldOrderByExpirationTimeOnCompareTo()
        {
            // Given
            long now = System.currentTimeMillis();
            ExpiringKey early = new ExpiringKey("a", 0, now + 10);
            ExpiringKey late = new ExpiringKey("b", 0, now + 50);
            
            // When / Then
            assertTrue(early.compareTo(late) < 0, "Earlier key should come before later key");
            assertTrue(late.compareTo(early) > 0, "Later key should come after earlier key");
        }
    }
}
