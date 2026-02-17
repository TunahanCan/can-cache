package com.cancache.agent.dashboard;

import java.time.Duration;

public final class Formatters {
    private Formatters() {
    }

    public static String humanBytes(long bytes) {
        String[] units = { "B", "KB", "MB", "GB", "TB" };
        double value = bytes;
        int i = 0;
        while (value >= 1024 && i < units.length - 1) {
            value = value / 1024.0;
            i++;
        }
        return i == 0 ? ((long) value) + units[i] : String.format("%.1f%s", value, units[i]);
    }

    public static String fmtDuration(Duration d) {
        long s = d.toSeconds();
        long h = s / 3600;
        long m = (s % 3600) / 60;
        long sec = s % 60;
        return String.format("%02d:%02d:%02d", h, m, sec);
    }

    public static String fmtSince(Duration d) {
        long ms = Math.max(0, d.toMillis());
        long sec = ms / 1000;
        long rem = ms % 1000;
        return String.format("%02d:%02d.%01d", sec / 60, sec % 60, rem / 100);
    }
}
