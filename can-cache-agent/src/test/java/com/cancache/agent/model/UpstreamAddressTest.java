package com.cancache.agent.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UpstreamAddressTest {

    @Test
    void shouldNormalizeAndRenderAddress() {
        UpstreamAddress address = UpstreamAddress.of(" cache-node-1 ", 11212);

        assertEquals("cache-node-1", address.host());
        assertEquals(11212, address.port());
        assertEquals("cache-node-1:11212", address.toString());
    }

    @Test
    void shouldParseHostPortAddress() {
        UpstreamAddress address = UpstreamAddress.parse("127.0.0.1:11212");

        assertEquals("127.0.0.1", address.host());
        assertEquals(11212, address.port());
    }

    @Test
    void shouldRejectInvalidAddresses() {
        assertThrows(IllegalArgumentException.class, () -> UpstreamAddress.of("", 11212));
        assertThrows(IllegalArgumentException.class, () -> UpstreamAddress.of("bad host", 11212));
        assertThrows(IllegalArgumentException.class, () -> UpstreamAddress.of("a".repeat(254), 11212));
        assertThrows(IllegalArgumentException.class, () -> UpstreamAddress.of("127.0.0.1", 0));
        assertThrows(IllegalArgumentException.class, () -> UpstreamAddress.parse("127.0.0.1"));
    }
}
