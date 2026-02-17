package com.cancache.agent.model;

import java.time.Duration;
import java.time.Instant;

public record ConnectionRecord(
        Instant start,
        Instant end,
        String client,
        String upstream,
        long bytesIn,
        long bytesOut) {

    public Duration duration() {
        return Duration.between(start, end == null ? Instant.now() : end);
    }
}
