package com.cancache.agent.dashboard;

import com.cancache.agent.config.AgentConfig;
import com.cancache.agent.service.ConnectionTracker;
import com.cancache.agent.service.DiscoveryService;
import com.cancache.agent.service.MetricsModel;
import com.cancache.agent.service.UpstreamRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class TuiDashboard {

    private static final Logger LOG = Logger.getLogger(TuiDashboard.class);

    @Inject Vertx vertx;
    @Inject AgentConfig config;
    @Inject UpstreamRegistry registry;
    @Inject MetricsModel metrics;
    @Inject ConnectionTracker tracker;
    @Inject DiscoveryService discoveryService;

    private long timerId = -1;
    private Thread keyboardThread;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private boolean tuiMode = false;
    private boolean compactLogMode = false;
    private final Object outputLock = new Object();

    public void start()
    {
        String mode = config.dashboard().mode().toLowerCase();
        tuiMode = switch (mode) {
            case "tui" -> true;
            case "log", "compact" -> false;
            default -> canUseTuiInAutoMode();
        };

        compactLogMode = "compact".equals(mode) || (!tuiMode && supportsInlineRefresh());
        running.set(true);

        if (tuiMode) {
            if (initTui()) {
                timerId = vertx.setPeriodic(config.dashboard().refresh().toMillis(), _ -> render());
                startKeyboardListener();
                LOG.info("dashboard mode=tui (ansi)");
            } else {
                tuiMode = false;
                compactLogMode = supportsInlineRefresh();
                timerId = vertx.setPeriodic(config.dashboard().snapshotInterval().toMillis(), id -> logSnapshot());
                LOG.infov("dashboard fallback mode=log compact={0}", compactLogMode);
            }
        } else {
            timerId = vertx.setPeriodic(config.dashboard().snapshotInterval().toMillis(), id -> logSnapshot());
            LOG.infov("dashboard mode=log compact={0}", compactLogMode);
        }
    }

    private boolean initTui() {
        if (!supportsInlineRefresh()) {
            return false;
        }
        synchronized (outputLock) {
            System.out.print("\033[?1049h\033[?25l");
            System.out.flush();
        }
        return true;
    }

    private void cleanupTui() {
        synchronized (outputLock) {
            System.out.print("\033[0m\033[?25h\033[?1049l");
            System.out.flush();
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (timerId != -1) {
            vertx.cancelTimer(timerId);
        }
        if (keyboardThread != null) {
            keyboardThread.interrupt();
        }
        if (tuiMode) {
            cleanupTui();
        }
        if (compactLogMode) {
            System.out.print("\n");
            System.out.flush();
        }
    }

    private void logSnapshot() {
        String line = String.format("[SNAP] up=%d/%d conns=%d in=%s out=%s",
            registry.upCount(), registry.total(), metrics.activeConnections(),
            Formatters.humanBytes(metrics.bytesIn()), Formatters.humanBytes(metrics.bytesOut()));
        if (compactLogMode) {
            System.out.print("\r\033[2K" + line);
            System.out.flush();
            return;
        }
        LOG.info(line);
    }

    private boolean canUseTuiInAutoMode() {
        if (System.console() != null) {
            return true;
        }
        if (isLikelyCiEnvironment()) {
            return false;
        }
        return supportsInlineRefresh();
    }

    private boolean supportsInlineRefresh() {
        String termName = System.getenv("TERM");
        return termName != null && !termName.isBlank() && !"dumb".equalsIgnoreCase(termName);
    }

    private boolean isLikelyCiEnvironment() {
        List<String> envKeys = List.of("CI",
                "GITHUB_ACTIONS",
                "GITLAB_CI",
                "JENKINS_URL",
                "BUILDKITE");
        return envKeys.stream().anyMatch(key -> {
            String value = System.getenv(key);
            return value != null && !value.isBlank();
        });
    }

    private void render() {
        try {
            int width = Math.max(80, terminalColumns());
            int rows = Math.max(24, terminalRows());
            int contentBottom = rows - 2;
            StringBuilder sb = new StringBuilder(4096);
            sb.append("\033[H\033[2J");

            int y = 0;
            y = drawTitle(sb, width, y);
            y = drawStatus(sb, width, y + 1, contentBottom);
            y = drawNodes(sb, width, y + 1, contentBottom);
            y = drawConnections(sb, width, y + 1, contentBottom);
            y = drawEvents(sb, width, y + 1, contentBottom);
            drawFooter(sb, width, rows - 1);
            sb.append("\033[0m");

            synchronized (outputLock) {
                System.out.print(sb);
                System.out.flush();
            }
        } catch (Exception e) {
            LOG.debug("TUI render failed", e);
        }
    }

    private int drawTitle(StringBuilder sb, int width, int startY) {
        String title = "CAN⚡CACHE LIVE CONTROL CENTER";
        appendAt(sb, startY, Math.max(0, (width - title.length()) / 2), cut(title, width));
        return startY + 1;
    }

    private int drawStatus(StringBuilder sb, int width, int startY, int contentBottom) {
        if (startY > contentBottom) {
            return startY;
        }
        appendAt(sb, startY, 0, "\033[1;33m" + cut("STATUS " + "-".repeat(Math.max(0, width - 7)), width));

        String uptime = Formatters.fmtDuration(Duration.between(metrics.startedAt(), Instant.now()));
        int up = registry.upCount();
        int total = registry.total();

        appendIfVisible(sb, startY + 1, 0, contentBottom,
            "\033[37m" + cut("Listen: " + config.listen().host() + ":" + config.listen().port(), width));
        appendIfVisible(sb, startY + 2, 0, contentBottom,
            cut("DNS: " + config.discovery().dns() + "    Nodes: " + up + "/" + total + " UP", width));
        appendIfVisible(sb, startY + 3, 0, contentBottom,
            cut("Policy: " + config.selection().policy() + "    Active: " + metrics.activeConnections(), width));
        appendIfVisible(sb, startY + 4, 0, contentBottom,
            cut("Traffic: ↓" + Formatters.humanBytes(metrics.bytesIn()) + "  ↑" + Formatters.humanBytes(metrics.bytesOut())
                + "    Uptime: " + uptime, width));

        return Math.min(contentBottom + 1, startY + 5);
    }

    private int drawNodes(StringBuilder sb, int width, int startY, int contentBottom) {
        if (startY > contentBottom) {
            return startY;
        }
        appendAt(sb, startY, 0, "\033[1;33m" + cut("NODES " + "-".repeat(Math.max(0, width - 6)), width));

        var nodes = registry.all();
        if (nodes.isEmpty()) {
            appendIfVisible(sb, startY + 1, 0, contentBottom, "\033[37m(no nodes)");
            return Math.min(contentBottom + 1, startY + 2);
        }

        appendIfVisible(sb, startY + 1, 0, contentBottom,
            "\033[37m" + cut("#  ADDRESS               STATE  CHECKED  CONN  TOTAL  IN      OUT", width));
        int y = startY + 2;
        int i = 1;
        int maxRows = Math.max(0, contentBottom - y + 1);
        for (var n : nodes) {
            if (maxRows-- <= 0) {
                break;
            }
            String line = String.format("%-2d %-20s %-6s %-7s %-5d %-6d %-7s %-7s",
                i++, cut(n.address(), 20), n.state(), Formatters.fmtSince(n.lastCheckAge()),
                n.activeConn(), n.totalConn(), Formatters.humanBytes(n.bytesIn()), Formatters.humanBytes(n.bytesOut()));
            appendAt(sb, y++, 0, cut(line, width));
        }
        return y;
    }

    private int drawConnections(StringBuilder sb, int width, int startY, int contentBottom) {
        if (startY > contentBottom) {
            return startY;
        }
        appendAt(sb, startY, 0, "\033[1;33m" + cut("CONNECTIONS " + "-".repeat(Math.max(0, width - 12)), width));

        var conns = tracker.latest();
        if (conns.isEmpty()) {
            appendIfVisible(sb, startY + 1, 0, contentBottom, "\033[37m(no connections)");
            return Math.min(contentBottom + 1, startY + 2);
        }

        appendIfVisible(sb, startY + 1, 0, contentBottom,
            "\033[37m" + cut("CLIENT             UPSTREAM           IN       OUT      DUR", width));
        int y = startY + 2;
        int maxRows = Math.max(0, contentBottom - y + 1);
        for (var rec : conns) {
            if (maxRows-- <= 0) {
                break;
            }
            String line = String.format("%-18s %-18s %-8s %-8s %-8s",
                cut(rec.client(), 18), cut(rec.upstream(), 18),
                Formatters.humanBytes(rec.bytesIn()), Formatters.humanBytes(rec.bytesOut()), Formatters.fmtSince(rec.duration()));
            appendAt(sb, y++, 0, cut(line, width));
        }
        return y;
    }

    private int drawEvents(StringBuilder sb, int width, int startY, int contentBottom) {
        if (startY > contentBottom) {
            return startY;
        }
        appendAt(sb, startY, 0, "\033[1;33m" + cut("EVENTS " + "-".repeat(Math.max(0, width - 8)), width));

        var events = metrics.latestEvents();
        if (events.isEmpty()) {
            appendIfVisible(sb, startY + 1, 0, contentBottom, "\033[37m(no events)");
            return Math.min(contentBottom + 1, startY + 2);
        }

        int y = startY + 1;
        int maxRows = Math.max(0, contentBottom - y + 1);
        for (var event : events) {
            if (maxRows-- <= 0) {
                break;
            }
            if (event.contains("ERR")) {
                appendAt(sb, y++, 0, "\033[31m" + cut(event, width));
            } else if (event.contains("WARN")) {
                appendAt(sb, y++, 0, "\033[33m" + cut(event, width));
            } else {
                appendAt(sb, y++, 0, "\033[37m" + cut(event, width));
            }
        }
        return y;
    }

    private void drawFooter(StringBuilder sb, int width, int y) {
        appendAt(sb, y, 0, "\033[36m" + cut("[q] Quit   [r] Refresh DNS   [h] Help", width));
    }

    private void appendIfVisible(StringBuilder sb, int row, int col, int contentBottom, String text) {
        if (row <= contentBottom) {
            appendAt(sb, row, col, text);
        }
    }

    private int terminalColumns() {
        return parseEnvDimension("COLUMNS", 120);
    }

    private int terminalRows() {
        return parseEnvDimension("LINES", 40);
    }

    private int parseEnvDimension(String key, int fallback) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            int v = Integer.parseInt(value);
            return v > 20 ? v : fallback;
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private void appendAt(StringBuilder sb, int row, int col, String text) {
        sb.append("\033[").append(row + 1).append(';').append(col + 1).append('H').append(text);
    }

    private String cut(String s, int len) {
        if (s == null) {
            return "";
        }
        if (len <= 0) {
            return "";
        }
        return s.length() <= len ? s : s.substring(0, Math.max(0, len - 2)) + "..";
    }

    private void startKeyboardListener() {
        keyboardThread = Thread.ofVirtual().name("tui-keyboard").start(() -> {
            while (running.get()) {
                try {
                    InputStream in = System.in;
                    if (in.available() <= 0) {
                        Thread.sleep(30);
                        continue;
                    }
                    int next = in.read();
                    if (next < 0 || next == 27) {
                        running.set(false);
                        cleanupTui();
                        System.exit(0);
                    }
                    char ch = (char) next;
                    if (ch == 'q' || ch == 'Q') {
                        running.set(false);
                        cleanupTui();
                        System.exit(0);
                    } else if (ch == 'r' || ch == 'R') {
                        discoveryService.refreshNowAsync();
                        metrics.addEvent("[INFO] DNS refresh triggered");
                    } else if (ch == 'h' || ch == 'H') {
                        metrics.addEvent("[HELP] q=quit, r=refresh, h=help");
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    if (running.get()) {
                        LOG.debug("Keyboard error", e);
                    }
                }
            }
        });
    }

    public JsonObject snapshotJson() {
        return new JsonObject()
            .put("up", registry.upCount())
            .put("total", registry.total())
            .put("activeConns", metrics.activeConnections())
            .put("bytesIn", metrics.bytesIn())
            .put("bytesOut", metrics.bytesOut());
    }
}
