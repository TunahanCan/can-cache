(() => {
    "use strict";

    const API_URL = "/agent/instances";
    const REFRESH_INTERVAL_MS = 3000;
    const MAX_TRAFFIC_SAMPLES = 31;
    const ROOT = document.documentElement;
    const $ = (id) => document.getElementById(id);

    const state = {
        autoRefresh: true,
        timer: null,
        countdownTimer: null,
        nextRefreshAt: 0,
        loading: false,
        hasData: false,
        nodeFilter: "ALL",
        lastPayload: null,
        trafficSamples: [],
        previousTraffic: null
    };

    const numberFormatter = new Intl.NumberFormat("tr-TR");
    const dateTimeFormatter = new Intl.DateTimeFormat("tr-TR", {
        dateStyle: "short",
        timeStyle: "medium"
    });
    const timeFormatter = new Intl.DateTimeFormat("tr-TR", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
    });

    function setText(id, value) {
        const target = $(id);
        if (target) {
            target.textContent = value;
        }
    }

    function finiteNumber(value, fallback = 0) {
        const numeric = Number(value);
        return Number.isFinite(numeric) ? numeric : fallback;
    }

    function safeArray(value) {
        return Array.isArray(value) ? value : [];
    }

    function formatNumber(value) {
        return numberFormatter.format(finiteNumber(value));
    }

    function formatBytes(value) {
        const bytes = Math.max(0, finiteNumber(value));
        if (bytes < 1024) {
            return `${formatNumber(bytes)} B`;
        }

        const units = ["KB", "MB", "GB", "TB", "PB"];
        let amount = bytes / 1024;
        let unitIndex = 0;
        while (amount >= 1024 && unitIndex < units.length - 1) {
            amount /= 1024;
            unitIndex += 1;
        }

        const digits = amount >= 100 ? 0 : amount >= 10 ? 1 : 2;
        return `${amount.toLocaleString("tr-TR", {
            maximumFractionDigits: digits
        })} ${units[unitIndex]}`;
    }

    function formatRate(value) {
        return `${formatBytes(value)}/sn`;
    }

    function formatDuration(totalSeconds) {
        let remaining = Math.max(0, Math.floor(finiteNumber(totalSeconds)));
        const days = Math.floor(remaining / 86400);
        remaining %= 86400;
        const hours = Math.floor(remaining / 3600);
        remaining %= 3600;
        const minutes = Math.floor(remaining / 60);
        const seconds = remaining % 60;

        if (days > 0) {
            return `${days}g ${hours}sa`;
        }
        if (hours > 0) {
            return `${hours}sa ${minutes}dk`;
        }
        if (minutes > 0) {
            return `${minutes}dk ${seconds}sn`;
        }
        return `${seconds}sn`;
    }

    function formatMilliseconds(milliseconds) {
        const value = Math.max(0, finiteNumber(milliseconds));
        if (value < 1000) {
            return `${Math.round(value)} ms`;
        }
        if (value < 60000) {
            return `${(value / 1000).toLocaleString("tr-TR", { maximumFractionDigits: 1 })} sn`;
        }
        return formatDuration(value / 1000);
    }

    function parseDate(value) {
        const date = new Date(value);
        return Number.isNaN(date.getTime()) ? null : date;
    }

    function formatDate(value, formatter = dateTimeFormatter) {
        const date = parseDate(value);
        return date ? formatter.format(date) : "—";
    }

    function formatAge(seconds) {
        const parsed = finiteNumber(seconds, -1);
        if (parsed < 0 || parsed > 315360000) {
            return "Henüz kontrol edilmedi";
        }
        const value = Math.floor(parsed);
        if (value < 5) {
            return "Az önce";
        }
        if (value < 60) {
            return `${value} sn önce`;
        }
        if (value < 3600) {
            return `${Math.floor(value / 60)} dk önce`;
        }
        if (value < 86400) {
            return `${Math.floor(value / 3600)} sa önce`;
        }
        return `${Math.floor(value / 86400)} gün önce`;
    }

    function makeElement(tag, className, text) {
        const element = document.createElement(tag);
        if (className) {
            element.className = className;
        }
        if (text !== undefined) {
            element.textContent = String(text);
        }
        return element;
    }

    function replaceChildren(target, children) {
        const fragment = document.createDocumentFragment();
        children.forEach((child) => fragment.append(child));
        target.replaceChildren(fragment);
    }

    function setSystemStatus(kind, label) {
        const status = $("system-status");
        status.className = `system-status system-status--${kind}`;
        setText("system-status-label", label);
    }

    function appendServiceState(targetId, enabled, enabledLabel, disabledLabel) {
        const target = $(targetId);
        const dot = makeElement("i", `state-dot state-dot--${enabled ? "up" : "down"}`);
        dot.setAttribute("aria-hidden", "true");
        target.replaceChildren(dot, document.createTextNode(enabled ? enabledLabel : disabledLabel));
    }

    function lifecycleView(value) {
        const normalized = String(value || "").toUpperCase();
        if (normalized === "READY") {
            return { label: "Trafiğe hazır", tone: "ready" };
        }
        if (normalized === "DRAINING") {
            return { label: "Güvenli kapanıyor", tone: "warning" };
        }
        if (normalized === "STARTING") {
            return { label: "Başlatılıyor", tone: "loading" };
        }
        return { label: "Sınırlı hizmet", tone: "warning" };
    }

    function normalizeState(value) {
        const normalized = String(value || "UNKNOWN").toUpperCase();
        return ["UP", "DOWN", "UNKNOWN"].includes(normalized) ? normalized : "UNKNOWN";
    }

    function stateLabel(value) {
        if (value === "UP") {
            return "Hazır";
        }
        if (value === "DOWN") {
            return "Sorunlu";
        }
        return "Bekliyor";
    }

    function renderSummary(data) {
        const total = Math.max(0, finiteNumber(data.totalInstances));
        const healthy = Math.min(total, Math.max(0, finiteNumber(data.healthyInstances)));
        const ratio = total > 0 ? healthy / total : 0;
        const percent = Math.round(ratio * 100);
        const bytesIn = Math.max(0, finiteNumber(data.bytesIn));
        const bytesOut = Math.max(0, finiteNumber(data.bytesOut));

        setText("healthy-instances", formatNumber(healthy));
        setText("total-instances", `/ ${formatNumber(total)}`);
        setText("active-connections", formatNumber(data.activeConnections));
        setText("bytes-in", formatBytes(bytesIn));
        setText("bytes-out", formatBytes(bytesOut));
        setText("total-traffic", `Toplam ${formatBytes(bytesIn + bytesOut)}`);
        setText("uptime", formatDuration(data.uptimeSeconds));
        setText("footer-uptime", `Çalışma süresi: ${formatDuration(data.uptimeSeconds)}`);
        setText("started-at", formatDate(data.startedAt, timeFormatter));
        setText("dns-changes", `Adres değişimi: ${formatNumber(data.dnsChanges)}`);
        setText("last-updated", formatDate(data.now));
        setText("connection-summary",
            `${formatNumber(data.pendingConnections)} kuruluyor · ${formatNumber(data.totalConnections)} toplam`);

        $("health-meter").style.width = `${percent}%`;
        setText("health-trend", total === 0 ? "Düğüm bekleniyor" : `${percent}% hazır`);

        if (data.listening === false) {
            setSystemStatus("error", "Trafik girişi kapalı");
        } else if (data.accepting === false) {
            setSystemStatus("warning", "Yeni trafik duraklatıldı");
        } else if (total === 0) {
            setSystemStatus("warning", "Düğüm bekleniyor");
        } else if (healthy === total) {
            setSystemStatus("healthy", "Sistem sağlıklı");
        } else if (healthy > 0) {
            setSystemStatus("warning", "Kısmi sorun");
        } else {
            setSystemStatus("error", "Hizmet verilemiyor");
        }
    }

    function renderServiceState(data) {
        const lifecycle = lifecycleView(data.state);
        const indicator = $("lifecycle-indicator");
        indicator.className = `service-light service-light--${lifecycle.tone}`;
        setText("lifecycle-state", lifecycle.label);

        appendServiceState("listener-state", data.listening === true && data.accepting !== false,
            "Trafik kabul ediyor", data.listening === false ? "Kapalı" : "Duraklatıldı");
        appendServiceState("registration-state", data.registrationListening === true,
            "Kayıt kabul ediyor", "Kapalı");

        setText("all-connections", formatNumber(data.totalConnections));
        setText("failover-count", formatNumber(data.failovers));
        setText("rejected-count", formatNumber(data.rejectedConnections));
        setText("dial-failure-count", formatNumber(data.dialFailures));
        setText("idle-timeout-count", formatNumber(data.idleTimeouts));
    }

    function renderHealth(data) {
        const instances = safeArray(data.instances);
        const counts = instances.reduce((result, instance) => {
            result[normalizeState(instance.state)] += 1;
            return result;
        }, { UP: 0, DOWN: 0, UNKNOWN: 0 });

        const total = counts.UP + counts.DOWN + counts.UNKNOWN;
        const percent = total > 0 ? Math.round((counts.UP / total) * 100) : 0;

        $("health-ring").style.setProperty("--health-percent", `${percent}%`);
        $("health-ring").setAttribute("aria-label", total > 0
            ? `Düğümlerin yüzde ${percent} kadarı sağlıklı`
            : "Henüz kayıtlı düğüm yok");
        setText("health-percent", total > 0 ? `%${percent}` : "—");
        setText("up-count", formatNumber(counts.UP));
        setText("down-count", formatNumber(counts.DOWN));
        setText("unknown-count", formatNumber(counts.UNKNOWN));

        let summary = "Henüz kayıtlı bir düğüm bulunmuyor.";
        if (total > 0 && counts.UP === total) {
            summary = `${total} düğümün tamamı trafiğe hazır.`;
        } else if (counts.UP > 0) {
            summary = `${counts.UP} düğüm hazır; ${counts.DOWN + counts.UNKNOWN} düğüm inceleme bekliyor.`;
        } else if (total > 0) {
            summary = "Trafiği karşılayabilecek sağlıklı düğüm bulunmuyor.";
        }
        setText("health-summary", summary);
    }

    function appendCell(row, content, className) {
        const cell = makeElement("td", className);
        if (content instanceof Node) {
            cell.append(content);
        } else {
            cell.textContent = String(content);
        }
        row.append(cell);
        return cell;
    }

    function stackedCell(main, sub) {
        const wrapper = makeElement("span");
        wrapper.append(
            makeElement("span", "cell-main", main),
            makeElement("span", "cell-sub", sub)
        );
        return wrapper;
    }

    function filteredInstances(instances) {
        if (state.nodeFilter === "UP") {
            return instances.filter((instance) => normalizeState(instance.state) === "UP");
        }
        if (state.nodeFilter === "ISSUE") {
            return instances.filter((instance) => normalizeState(instance.state) !== "UP");
        }
        return instances;
    }

    function renderNodes(data) {
        const allInstances = safeArray(data.instances);
        const instances = filteredInstances(allInstances);
        const rows = instances.map((instance) => {
            const row = document.createElement("tr");
            const nodeState = normalizeState(instance.state);

            const badge = makeElement("span", `status-badge status-badge--${nodeState.toLowerCase()}`,
                stateLabel(nodeState));
            appendCell(row, badge);

            const address = makeElement("span", "address", instance.address || "—");
            appendCell(row, address);

            appendCell(row, stackedCell(
                formatNumber(instance.activeConnections),
                finiteNumber(instance.pendingConnections) > 0
                    ? `${formatNumber(instance.pendingConnections)} kuruluyor`
                    : "bağlantı beklemiyor"
            ));
            appendCell(row, formatNumber(instance.totalConnections));
            appendCell(row, stackedCell(
                formatBytes(instance.bytesIn),
                `gelen · ${formatBytes(instance.bytesOut)} giden`
            ));
            appendCell(row, stackedCell(
                formatAge(instance.lastCheckAgeSeconds),
                formatDate(instance.lastCheck, timeFormatter)
            ));

            const rawError = String(instance.lastError || "—");
            const errorText = rawError === "-" ? "—" : rawError;
            const error = makeElement("span", errorText === "—" ? "" : "error-text", errorText);
            if (errorText !== "—") {
                error.title = errorText;
            }
            appendCell(row, error);
            return row;
        });

        replaceChildren($("nodes-body"), rows);
        $("nodes-empty").hidden = instances.length !== 0;
        $("nodes-table").hidden = instances.length === 0;

        if (allInstances.length > 0 && instances.length === 0) {
            const empty = $("nodes-empty");
            const title = empty.querySelector("strong");
            const copy = empty.querySelector("p");
            title.textContent = "Bu filtrede düğüm yok";
            copy.textContent = "Farklı durumdaki düğümleri görmek için filtreyi değiştirin.";
        } else {
            const empty = $("nodes-empty");
            const title = empty.querySelector("strong");
            const copy = empty.querySelector("p");
            title.textContent = "Henüz bir düğüm yok";
            copy.textContent = "Bir düğüm kaydolduğunda sağlık ve trafik bilgileri burada görünecek.";
        }
    }

    function renderConnections(data) {
        const connections = safeArray(data.recentConnections);
        const rows = connections.map((connection) => {
            const row = document.createElement("tr");
            appendCell(row, makeElement("span", "address", connection.client || "—"));
            appendCell(row, makeElement("span", "address", connection.upstream || "—"));
            appendCell(row, formatMilliseconds(connection.durationMs));
            appendCell(row, stackedCell(
                formatBytes(connection.bytesIn),
                `gelen · ${formatBytes(connection.bytesOut)} giden`
            ));
            appendCell(row, formatDate(connection.end, timeFormatter));
            return row;
        });

        replaceChildren($("connections-body"), rows);
        $("connections-empty").hidden = connections.length !== 0;
        $("connections-table").hidden = connections.length === 0;
        setText("connection-count", `${connections.length} kayıt`);
    }

    function eventTone(event) {
        const raw = String(event);
        if (/^\[(ERR|WARN)/i.test(raw)) {
            return "error";
        }
        if (/^\[(READY|REG|CONN)/i.test(raw)) {
            return "success";
        }
        const text = raw.toLocaleLowerCase("tr-TR");
        if (/(error|fail|down|hata|başarısız|kapandı|reddedildi)/u.test(text)) {
            return "error";
        }
        if (/(up|ready|start|registered|hazır|başladı|kayd)/u.test(text)) {
            return "success";
        }
        return "info";
    }

    function friendlyEvent(event) {
        const raw = String(event).replace(/^\[[^\]]+\]\s*/u, "");
        const translations = [
            [/^proxy listening on /i, "Trafik girişi hazır: "],
            [/^registered upstream=/i, "Düğüm kaydoldu: "],
            [/^registration connection limit reached/i, "Düğüm kayıt sınırına ulaşıldı"],
            [/^unauthorized registration attempt/i, "Yetkisiz düğüm kaydı engellendi"],
            [/^connection rejected /i, "Bağlantı reddedildi: "],
            [/^idle connection closed /i, "Boşta kalan bağlantı kapatıldı: "],
            [/^proxy draining active=/i, "Agent güvenli kapanıyor; açık bağlantı: "],
            [/^pending connection limit reached /i, "Bekleyen bağlantı sınırına ulaşıldı: "],
            [/^no reachable upstream /i, "Ulaşılabilir düğüm bulunamadı: "],
            [/^dial failed /i, "Düğüme bağlanılamadı: "],
            [/^upstream list updated /i, "Düğüm listesi güncellendi: "],
            [/^DNS refresh triggered/i, "Düğüm listesi yenileniyor"]
        ];

        for (const [pattern, replacement] of translations) {
            if (pattern.test(raw)) {
                return raw.replace(pattern, replacement);
            }
        }
        return raw;
    }

    function renderEvents(data) {
        const events = safeArray(data.latestEvents);
        const items = events.map((event, index) => {
            const item = makeElement("li", `timeline__item timeline__item--${eventTone(event)}`);
            const dot = makeElement("span", "timeline__dot");
            dot.setAttribute("aria-hidden", "true");
            const content = makeElement("div");
            content.append(
                makeElement("p", "timeline__text", friendlyEvent(event)),
                makeElement("span", "timeline__order", index === 0 ? "En yeni olay" : `${index + 1}. olay`)
            );
            item.append(dot, content);
            return item;
        });

        replaceChildren($("events-list"), items);
        $("events-empty").hidden = events.length !== 0;
        $("events-list").hidden = events.length === 0;
    }

    function addTrafficSample(data) {
        const now = parseDate(data.now)?.getTime() || Date.now();
        const bytesIn = Math.max(0, finiteNumber(data.bytesIn));
        const bytesOut = Math.max(0, finiteNumber(data.bytesOut));
        let rateIn = 0;
        let rateOut = 0;

        if (state.previousTraffic) {
            const elapsedSeconds = Math.max(.001, (now - state.previousTraffic.time) / 1000);
            rateIn = Math.max(0, (bytesIn - state.previousTraffic.bytesIn) / elapsedSeconds);
            rateOut = Math.max(0, (bytesOut - state.previousTraffic.bytesOut) / elapsedSeconds);
        }

        state.previousTraffic = { time: now, bytesIn, bytesOut };
        state.trafficSamples.push({ rateIn, rateOut });
        if (state.trafficSamples.length > MAX_TRAFFIC_SAMPLES) {
            state.trafficSamples.shift();
        }

        renderTrafficChart();
    }

    function linePath(samples, key, width, height, padding, maxValue) {
        const denominator = Math.max(1, samples.length - 1);
        return samples.map((sample, index) => {
            const x = padding + ((width - padding * 2) * index / denominator);
            const y = height - padding - ((height - padding * 2) * sample[key] / maxValue);
            return `${index === 0 ? "M" : "L"}${x.toFixed(2)} ${y.toFixed(2)}`;
        }).join(" ");
    }

    function renderTrafficChart() {
        const samples = state.trafficSamples;
        const latest = samples[samples.length - 1] || { rateIn: 0, rateOut: 0 };
        const maxRate = Math.max(1, ...samples.flatMap((sample) => [sample.rateIn, sample.rateOut]));

        setText("rate-in", formatRate(latest.rateIn));
        setText("rate-out", formatRate(latest.rateOut));
        setText("chart-peak", `En yüksek: ${formatRate(maxRate === 1 ? 0 : maxRate)}`);

        const chart = $("traffic-chart");
        chart.setAttribute("aria-label",
            `Anlık gelen trafik ${formatRate(latest.rateIn)}, giden trafik ${formatRate(latest.rateOut)}`);

        if (samples.length < 2) {
            $("traffic-line-in").setAttribute("d", "");
            $("traffic-line-out").setAttribute("d", "");
            $("traffic-area-in").setAttribute("d", "");
            $("chart-empty").hidden = false;
            return;
        }

        const width = 640;
        const height = 200;
        const padding = 8;
        const inPath = linePath(samples, "rateIn", width, height, padding, maxRate);
        const outPath = linePath(samples, "rateOut", width, height, padding, maxRate);
        const lastX = width - padding;
        const baseY = height - padding;

        $("traffic-line-in").setAttribute("d", inPath);
        $("traffic-line-out").setAttribute("d", outPath);
        $("traffic-area-in").setAttribute("d", `${inPath} L${lastX} ${baseY} L${padding} ${baseY} Z`);
        $("chart-empty").hidden = maxRate > 1;
    }

    function render(data) {
        state.lastPayload = data;
        renderSummary(data);
        renderServiceState(data);
        renderHealth(data);
        renderNodes(data);
        renderConnections(data);
        renderEvents(data);
        addTrafficSample(data);
    }

    function humanizeError(error) {
        if (error && error.name === "AbortError") {
            return "Yanıt zaman aşımına uğradı. Agent çalışıyor mu kontrol edin.";
        }
        return "Agent yanıt vermedi. Son veriler gösteriliyor.";
    }

    function showError(error) {
        $("error-banner").hidden = false;
        setText("error-message", humanizeError(error));
        setSystemStatus("error", state.hasData ? "Bağlantı koptu" : "Panele ulaşılamıyor");
    }

    function hideError() {
        $("error-banner").hidden = true;
    }

    async function loadStatus() {
        if (state.loading) {
            return;
        }

        state.loading = true;
        $("refresh-button").classList.add("is-spinning");
        $("refresh-button").setAttribute("aria-busy", "true");

        const controller = new AbortController();
        const timeout = window.setTimeout(() => controller.abort(), 5000);

        try {
            const response = await fetch(API_URL, {
                headers: { Accept: "application/json" },
                cache: "no-store",
                signal: controller.signal
            });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const payload = await response.json();
            if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
                throw new TypeError("Geçersiz agent yanıtı");
            }

            state.hasData = true;
            hideError();
            render(payload);
        } catch (error) {
            showError(error);
        } finally {
            window.clearTimeout(timeout);
            state.loading = false;
            $("refresh-button").classList.remove("is-spinning");
            $("refresh-button").removeAttribute("aria-busy");
            scheduleNextRefresh();
        }
    }

    function updateCountdown() {
        if (!state.autoRefresh) {
            setText("next-refresh", "Otomatik yenileme duraklatıldı");
            return;
        }
        const seconds = Math.max(0, Math.ceil((state.nextRefreshAt - Date.now()) / 1000));
        setText("next-refresh", seconds > 0 ? `${seconds} saniye sonra yenilenecek` : "Yenileniyor…");
    }

    function scheduleNextRefresh() {
        window.clearTimeout(state.timer);
        if (!state.autoRefresh) {
            updateCountdown();
            return;
        }
        state.nextRefreshAt = Date.now() + REFRESH_INTERVAL_MS;
        updateCountdown();
        state.timer = window.setTimeout(loadStatus, REFRESH_INTERVAL_MS);
    }

    function setAutoRefresh(enabled) {
        state.autoRefresh = enabled;
        const button = $("auto-refresh-button");
        button.setAttribute("aria-pressed", String(enabled));
        button.title = enabled ? "Otomatik yenilemeyi duraklat" : "Otomatik yenilemeyi başlat";
        setText("auto-refresh-label", enabled ? "Canlı" : "Duraklatıldı");

        if (enabled) {
            scheduleNextRefresh();
        } else {
            window.clearTimeout(state.timer);
            updateCountdown();
        }
    }

    function applyTheme(theme) {
        ROOT.dataset.theme = theme;
        const dark = theme === "dark";
        document.querySelector('meta[name="theme-color"]').content = dark ? "#0c1020" : "#f5f7fb";
        $("theme-button").title = dark ? "Açık görünüme geç" : "Koyu görünüme geç";
    }

    function initializeTheme() {
        let savedTheme = null;
        try {
            savedTheme = window.localStorage.getItem("can-cache-agent-theme");
        } catch (_) {
            // Storage may be blocked; the system preference is a safe fallback.
        }
        const preferred = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
        applyTheme(savedTheme === "dark" || savedTheme === "light" ? savedTheme : preferred);
    }

    function toggleTheme() {
        const next = ROOT.dataset.theme === "dark" ? "light" : "dark";
        applyTheme(next);
        try {
            window.localStorage.setItem("can-cache-agent-theme", next);
        } catch (_) {
            // Theme still applies for the current page when storage is unavailable.
        }
    }

    function initializeInteractions() {
        $("refresh-button").addEventListener("click", loadStatus);
        $("error-retry-button").addEventListener("click", loadStatus);
        $("auto-refresh-button").addEventListener("click", () => setAutoRefresh(!state.autoRefresh));
        $("theme-button").addEventListener("click", toggleTheme);

        document.querySelectorAll("[data-node-filter]").forEach((button) => {
            button.addEventListener("click", () => {
                state.nodeFilter = button.dataset.nodeFilter || "ALL";
                document.querySelectorAll("[data-node-filter]").forEach((candidate) => {
                    const active = candidate === button;
                    candidate.classList.toggle("is-active", active);
                    candidate.setAttribute("aria-pressed", String(active));
                });
                if (state.lastPayload) {
                    renderNodes(state.lastPayload);
                }
            });
        });

        document.addEventListener("visibilitychange", () => {
            if (document.visibilityState === "visible" && state.autoRefresh) {
                loadStatus();
            }
        });

        state.countdownTimer = window.setInterval(updateCountdown, 1000);
    }

    initializeTheme();
    initializeInteractions();
    loadStatus();
})();
