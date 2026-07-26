package com.cancache.agent.model;

import java.util.Objects;
import java.util.regex.Pattern;

public record UpstreamAddress(String host, int port) implements Comparable<UpstreamAddress> {
    private static final int MAX_HOST_LENGTH = 253;
    private static final Pattern HOST = Pattern.compile("^[a-zA-Z0-9_.-]+$");

    public UpstreamAddress {
        host = normalizeHost(host);
        if (host.length() > MAX_HOST_LENGTH || !HOST.matcher(host).matches()) {
            throw new IllegalArgumentException("Invalid upstream host: " + host);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid upstream port: " + port);
        }
    }

    public static UpstreamAddress of(String host, int port) {
        return new UpstreamAddress(host, port);
    }

    public static UpstreamAddress parse(String value) {
        String address = Objects.requireNonNull(value, "value").trim();
        int separator = address.lastIndexOf(':');
        if (separator <= 0 || separator == address.length() - 1) {
            throw new IllegalArgumentException("Invalid upstream address: " + value);
        }
        return new UpstreamAddress(address.substring(0, separator),
                Integer.parseInt(address.substring(separator + 1)));
    }

    private static String normalizeHost(String host) {
        String value = Objects.requireNonNull(host, "host").trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Upstream host must not be blank");
        }
        return value;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public int compareTo(UpstreamAddress other) {
        int hostComparison = host.compareTo(other.host);
        if (hostComparison != 0) {
            return hostComparison;
        }
        return Integer.compare(port, other.port);
    }
}
