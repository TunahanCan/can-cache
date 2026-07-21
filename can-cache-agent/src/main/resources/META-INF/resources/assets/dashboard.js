(() => {
  'use strict';

  const API_URL = '/agent/instances';
  const MAX_HISTORY = 48;
  const state = {
    timer: null,
    failures: 0,
    previous: null,
    startedAt: null,
    connectionHistory: [],
    throughputHistory: []
  };

  const $ = id => document.getElementById(id);

  function text(id, value) {
    const element = $(id);
    if (element) element.textContent = value;
  }

  function formatBytes(value, suffix = '') {
    const bytes = Number(value) || 0;
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    if (bytes <= 0) return `0 B${suffix}`;
    const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const scaled = bytes / Math.pow(1024, exponent);
    const digits = scaled >= 100 || exponent === 0 ? 0 : scaled >= 10 ? 1 : 2;
    return `${scaled.toFixed(digits)} ${units[exponent]}${suffix}`;
  }

  function formatDuration(totalSeconds) {
    let seconds = Math.max(0, Number(totalSeconds) || 0);
    const days = Math.floor(seconds / 86400);
    seconds %= 86400;
    const hours = Math.floor(seconds / 3600);
    seconds %= 3600;
    const minutes = Math.floor(seconds / 60);
    if (days) return `${days}g ${hours}s`;
    if (hours) return `${hours}s ${minutes}dk`;
    if (minutes) return `${minutes}dk ${Math.floor(seconds % 60)}sn`;
    return `${Math.floor(seconds)}sn`;
  }

  function formatClock(value) {
    if (!value) return '—';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '—';
    return new Intl.DateTimeFormat('tr-TR', {
      hour: '2-digit', minute: '2-digit', second: '2-digit'
    }).format(date);
  }

  function formatRelative(seconds) {
    const value = Math.max(0, Number(seconds) || 0);
    if (value < 2) return 'şimdi';
    if (value < 60) return `${Math.floor(value)} sn önce`;
    if (value < 3600) return `${Math.floor(value / 60)} dk önce`;
    return `${Math.floor(value / 3600)} sa önce`;
  }

  function shortAddress(address, max = 24) {
    const value = String(address || 'unknown');
    return value.length <= max ? value : `${value.slice(0, max - 1)}…`;
  }

  function appendHistory(history, value) {
    history.push(Math.max(0, Number(value) || 0));
    while (history.length > MAX_HISTORY) history.shift();
  }

  function calculateThroughput(snapshot) {
    if (!state.previous || state.startedAt !== snapshot.startedAt) return 0;
    const now = new Date(snapshot.now).getTime();
    const previousAt = new Date(state.previous.now).getTime();
    const elapsed = (now - previousAt) / 1000;
    if (!Number.isFinite(elapsed) || elapsed <= 0) return 0;
    const currentBytes = (Number(snapshot.bytesIn) || 0) + (Number(snapshot.bytesOut) || 0);
    const previousBytes = (Number(state.previous.bytesIn) || 0) + (Number(state.previous.bytesOut) || 0);
    return Math.max(0, (currentBytes - previousBytes) / elapsed);
  }

  function render(snapshot) {
    const instances = Array.isArray(snapshot.instances) ? snapshot.instances : [];
    const healthy = Number(snapshot.healthyInstances) || 0;
    const total = Number(snapshot.totalInstances) || 0;
    const healthPercent = total ? Math.round((healthy / total) * 100) : 0;
    const throughput = calculateThroughput(snapshot);

    appendHistory(state.connectionHistory, snapshot.activeConnections);
    appendHistory(state.throughputHistory, throughput);

    renderLiveStatus(true);
    text('last-sync', formatClock(snapshot.now));
    text('health-percent', total ? `${healthPercent}%` : '—');
    text('health-ratio', `${healthy} / ${total} pod`);
    text('metric-healthy', `${healthy}/${total}`);
    text('metric-healthy-note', total === 0 ? 'Pod keşfi bekleniyor' : healthy === total ? 'Tüm upstream podlar erişilebilir' : `${total - healthy} pod dikkat istiyor`);
    text('metric-connections', String(Number(snapshot.activeConnections) || 0));
    text('metric-throughput', formatBytes(throughput, '/s'));
    text('metric-traffic', formatBytes((Number(snapshot.bytesIn) || 0) + (Number(snapshot.bytesOut) || 0)));
    text('metric-throughput-note', `↓ ${formatBytes(rateFor('bytesIn', snapshot), '/s')} · ↑ ${formatBytes(rateFor('bytesOut', snapshot), '/s')}`);
    text('metric-traffic-note', `↓ ${formatBytes(snapshot.bytesIn)} · ↑ ${formatBytes(snapshot.bytesOut)}`);
    text('uptime', `Uptime ${formatDuration(snapshot.uptimeSeconds)}`);
    text('dns-changes', String(Number(snapshot.dnsChanges) || 0));
    text('chart-connection-value', String(Number(snapshot.activeConnections) || 0));
    text('chart-throughput-value', formatBytes(throughput, '/s'));

    const ring = $('health-ring');
    if (ring) {
      const circumference = 2 * Math.PI * 82;
      ring.style.strokeDasharray = String(circumference);
      ring.style.strokeDashoffset = String(circumference * (1 - healthPercent / 100));
      ring.classList.toggle('degraded', total > 0 && healthy < total);
    }

    const agent = snapshot.agent || {};
    text('listen-address', `Proxy ${agent.listenAddress || '—'}`);
    text('selection-policy', `Policy ${agent.selectionPolicy || '—'}`);
    text('discovery-target', `Discovery ${agent.discoveryTarget || '—'}`);
    text('health-interval', agent.healthIntervalMillis ? `${agent.healthIntervalMillis} ms` : '—');
    text('registration-status', agent.registrationEnabled ? `ON · ${agent.registrationAddress || ''}` : 'OFF');

    if (!total) {
      text('hero-summary', 'Agent hazır; DNS veya registration üzerinden cache podları bekleniyor.');
    } else if (healthy === total) {
      text('hero-summary', `${total} cache podunun tamamı erişilebilir. Trafik agent üzerinden dengeli biçimde akıyor.`);
    } else {
      text('hero-summary', `${healthy} pod erişilebilir, ${total - healthy} pod için sağlık kontrolü gerekiyor.`);
    }

    renderTopology(instances, agent, snapshot.activeConnections);
    renderPods(instances);
    renderEvents(snapshot.latestEvents);
    renderConnections(snapshot.recentConnections);
    drawSparkline('connections-chart', state.connectionHistory, 'sky');
    drawSparkline('throughput-chart', state.throughputHistory, 'mint');

    state.startedAt = snapshot.startedAt;
    state.previous = snapshot;
  }

  function rateFor(field, snapshot) {
    if (!state.previous || state.startedAt !== snapshot.startedAt) return 0;
    const elapsed = (new Date(snapshot.now).getTime() - new Date(state.previous.now).getTime()) / 1000;
    if (!Number.isFinite(elapsed) || elapsed <= 0) return 0;
    return Math.max(0, ((Number(snapshot[field]) || 0) - (Number(state.previous[field]) || 0)) / elapsed);
  }

  function renderLiveStatus(connected, message) {
    const pill = $('live-pill');
    const banner = $('connection-banner');
    if (!pill || !banner) return;
    pill.classList.toggle('online', connected);
    pill.classList.toggle('connecting', !connected);
    text('live-label', connected ? 'LIVE' : 'RECONNECTING');
    banner.classList.toggle('hidden', connected);
    if (!connected) text('connection-error', message || 'Yeniden bağlanılıyor…');
  }

  function svgElement(name, attributes = {}) {
    const element = document.createElementNS('http://www.w3.org/2000/svg', name);
    Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, String(value)));
    return element;
  }

  function renderTopology(instances, agent, activeConnections) {
    const svg = $('topology');
    const empty = $('topology-empty');
    if (!svg || !empty) return;
    svg.replaceChildren();
    const hasNodes = instances.length > 0;
    empty.classList.toggle('hidden', hasNodes);
    svg.classList.toggle('hidden', !hasNodes);
    if (!hasNodes) return;

    const defs = svgElement('defs');
    const glow = svgElement('filter', {id: 'soft-glow', x: '-60%', y: '-60%', width: '220%', height: '220%'});
    glow.append(svgElement('feGaussianBlur', {stdDeviation: '7', result: 'blur'}));
    const merge = svgElement('feMerge');
    merge.append(svgElement('feMergeNode', {in: 'blur'}), svgElement('feMergeNode', {in: 'SourceGraphic'}));
    glow.append(merge);
    defs.append(glow);
    svg.append(defs);

    const center = {x: 450, y: 215};
    const positions = topologyPositions(instances.length, center);
    instances.forEach((node, index) => {
      const position = positions[index];
      const stateName = String(node.state || 'UNKNOWN').toLowerCase();
      const line = svgElement('line', {
        x1: center.x, y1: center.y, x2: position.x, y2: position.y,
        class: `topology-link ${stateName}`,
        'stroke-width': Math.min(6, 1.5 + (Number(node.activeConnections) || 0) * 0.45)
      });
      svg.append(line);
    });

    const agentGroup = svgElement('g', {class: 'topology-agent', transform: `translate(${center.x} ${center.y})`});
    agentGroup.append(svgElement('circle', {r: 66, class: 'agent-halo'}));
    agentGroup.append(svgElement('circle', {r: 48, class: 'agent-core', filter: 'url(#soft-glow)'}));
    const bolt = svgElement('path', {d: 'M -6 -27 L 20 -27 L 5 -5 L 22 -5 L -13 31 L -3 6 L -21 6 Z', class: 'agent-bolt'});
    agentGroup.append(bolt);
    const agentLabel = svgElement('text', {y: 83, class: 'topology-agent-label', 'text-anchor': 'middle'});
    agentLabel.textContent = 'CAN CACHE AGENT';
    const agentSub = svgElement('text', {y: 101, class: 'topology-agent-sub', 'text-anchor': 'middle'});
    agentSub.textContent = `${Number(activeConnections) || 0} active · ${agent.selectionPolicy || 'RR'}`;
    agentGroup.append(agentLabel, agentSub);
    svg.append(agentGroup);

    instances.forEach((node, index) => {
      const position = positions[index];
      const stateName = String(node.state || 'UNKNOWN').toLowerCase();
      const group = svgElement('g', {
        class: `topology-node ${stateName}`,
        transform: `translate(${position.x} ${position.y})`,
        tabindex: '0', role: 'group',
        'aria-label': `${node.address}, ${node.state}, ${node.activeConnections} aktif bağlantı`
      });
      group.append(svgElement('circle', {r: 39, class: 'node-halo'}));
      group.append(svgElement('circle', {r: 31, class: 'node-core'}));
      group.append(svgElement('rect', {x: -13, y: -13, width: 26, height: 26, rx: 7, class: 'node-chip'}));
      group.append(svgElement('path', {d: 'M-7 -5 H7 M-7 0 H7 M-7 5 H3', class: 'node-chip-lines'}));
      group.append(svgElement('circle', {cx: 24, cy: -24, r: 6, class: 'node-status'}));
      const label = svgElement('text', {y: 53, class: 'topology-node-label', 'text-anchor': 'middle'});
      label.textContent = shortAddress(node.address, 23);
      const detail = svgElement('text', {y: 69, class: 'topology-node-detail', 'text-anchor': 'middle'});
      detail.textContent = `${node.activeConnections || 0} conn · ${node.latencyMillis >= 0 ? `${node.latencyMillis} ms` : 'probing'}`;
      const title = svgElement('title');
      title.textContent = `${node.address} — ${node.state}`;
      group.append(label, detail, title);
      svg.append(group);
    });
  }

  function topologyPositions(count, center) {
    const positions = [];
    for (let index = 0; index < count; index++) {
      const ring = count <= 8 ? 0 : index < 8 ? 0 : 1;
      const ringStart = ring === 0 ? 0 : 8;
      const ringCount = ring === 0 ? Math.min(count, 8) : count - 8;
      const ringIndex = index - ringStart;
      const angle = -Math.PI / 2 + (Math.PI * 2 * ringIndex) / Math.max(1, ringCount);
      const radiusX = ring === 0 ? 315 : 395;
      const radiusY = ring === 0 ? 150 : 190;
      positions.push({x: center.x + Math.cos(angle) * radiusX, y: center.y + Math.sin(angle) * radiusY});
    }
    return positions;
  }

  function renderPods(instances) {
    const container = $('pod-cards');
    if (!container) return;
    container.replaceChildren();
    text('pod-count', `${instances.length} pod`);
    if (!instances.length) {
      const empty = document.createElement('div');
      empty.className = 'pod-empty';
      empty.textContent = 'Keşfedilmiş cache podu bulunmuyor.';
      container.append(empty);
      return;
    }

    instances.forEach((node, index) => {
      const card = document.createElement('article');
      const stateName = String(node.state || 'UNKNOWN').toLowerCase();
      card.className = `pod-card ${stateName}`;

      const top = document.createElement('div');
      top.className = 'pod-card-top';
      const identity = document.createElement('div');
      identity.className = 'pod-identity';
      const icon = document.createElement('span');
      icon.className = 'pod-icon';
      icon.textContent = String(index + 1).padStart(2, '0');
      const labels = document.createElement('div');
      const heading = document.createElement('h3');
      heading.textContent = node.address || 'unknown';
      const source = document.createElement('p');
      source.textContent = node.source || 'UNKNOWN';
      labels.append(heading, source);
      identity.append(icon, labels);
      const badge = document.createElement('span');
      badge.className = `state-badge ${stateName}`;
      badge.textContent = stateLabel(node.state);
      top.append(identity, badge);

      const stats = document.createElement('dl');
      stats.className = 'pod-stat-grid';
      appendDefinition(stats, 'Latency', node.latencyMillis >= 0 ? `${node.latencyMillis} ms` : '—');
      appendDefinition(stats, 'Active', String(node.activeConnections || 0));
      appendDefinition(stats, 'Traffic', formatBytes((Number(node.bytesIn) || 0) + (Number(node.bytesOut) || 0)));
      appendDefinition(stats, 'Errors', String(node.errorCount || 0));

      const checks = (Number(node.successfulChecks) || 0) + (Number(node.failedChecks) || 0);
      const availability = checks ? ((Number(node.successfulChecks) || 0) / checks) * 100 : 0;
      const footer = document.createElement('div');
      footer.className = 'pod-card-footer';
      const availabilityText = document.createElement('span');
      availabilityText.textContent = checks ? `Probe başarısı %${availability.toFixed(1)}` : 'Probe verisi bekleniyor';
      const checked = document.createElement('span');
      checked.textContent = checks ? `Kontrol: ${formatRelative(node.lastCheckAgeSeconds)}` : 'İlk probe bekleniyor';
      footer.append(availabilityText, checked);

      const bar = document.createElement('div');
      bar.className = 'availability-bar';
      const fill = document.createElement('i');
      fill.style.width = `${checks ? availability : 0}%`;
      bar.append(fill);
      card.append(top, stats, bar, footer);
      if (stateName === 'down' && node.lastError && node.lastError !== '-') {
        const error = document.createElement('p');
        error.className = 'pod-error';
        error.textContent = node.lastError;
        card.append(error);
      }
      container.append(card);
    });
  }

  function appendDefinition(parent, label, value) {
    const wrapper = document.createElement('div');
    const term = document.createElement('dt');
    const description = document.createElement('dd');
    term.textContent = label;
    description.textContent = value;
    wrapper.append(term, description);
    parent.append(wrapper);
  }

  function stateLabel(value) {
    switch (String(value || '').toUpperCase()) {
      case 'UP': return 'ERİŞİLEBİLİR';
      case 'DOWN': return 'DOWN';
      default: return 'PROBING';
    }
  }

  function renderEvents(events) {
    const feed = $('event-feed');
    if (!feed) return;
    feed.replaceChildren();
    const values = Array.isArray(events) ? events : [];
    if (!values.length) {
      const item = document.createElement('li');
      item.className = 'event-empty';
      item.textContent = 'Henüz agent olayı yok.';
      feed.append(item);
      return;
    }
    values.slice(0, 10).forEach((event, index) => {
      const value = String(event);
      const item = document.createElement('li');
      item.className = value.includes('ERR') ? 'error' : value.includes('WARN') ? 'warn' : 'info';
      const marker = document.createElement('span');
      marker.className = 'event-marker';
      const copy = document.createElement('div');
      const message = document.createElement('p');
      message.textContent = value;
      const time = document.createElement('small');
      time.textContent = index === 0 ? 'en yeni' : `olay ${index + 1}`;
      copy.append(message, time);
      item.append(marker, copy);
      feed.append(item);
    });
  }

  function renderConnections(connections) {
    const body = $('connections-body');
    const empty = $('connections-empty');
    if (!body || !empty) return;
    body.replaceChildren();
    const values = Array.isArray(connections) ? connections : [];
    empty.classList.toggle('hidden', values.length > 0);
    values.forEach(connection => {
      const row = document.createElement('tr');
      appendCell(row, connection.client || '—', 'mono');
      appendCell(row, connection.upstream || '—', 'mono accent');
      appendCell(row, formatClock(connection.start));
      appendCell(row, `${Number(connection.durationMs) || 0} ms`);
      appendCell(row, formatBytes(connection.bytesIn));
      appendCell(row, formatBytes(connection.bytesOut));
      body.append(row);
    });
  }

  function appendCell(row, value, className = '') {
    const cell = document.createElement('td');
    cell.textContent = value;
    if (className) cell.className = className;
    row.append(cell);
  }

  function drawSparkline(id, values, tone) {
    const svg = $(id);
    if (!svg) return;
    svg.replaceChildren();
    const width = 420;
    const height = 92;
    const padding = 4;
    const data = values.length > 1 ? values : [0, ...(values.length ? values : [0])];
    const max = Math.max(1, ...data);
    const points = data.map((value, index) => {
      const x = padding + (index / Math.max(1, data.length - 1)) * (width - padding * 2);
      const y = height - padding - (value / max) * (height - padding * 2);
      return [x, y];
    });
    const linePath = points.map(([x, y], index) => `${index ? 'L' : 'M'} ${x.toFixed(2)} ${y.toFixed(2)}`).join(' ');
    const areaPath = `${linePath} L ${points.at(-1)[0].toFixed(2)} ${height} L ${points[0][0].toFixed(2)} ${height} Z`;
    const gradientId = `${id}-gradient`;
    const defs = svgElement('defs');
    const gradient = svgElement('linearGradient', {id: gradientId, x1: '0', y1: '0', x2: '0', y2: '1'});
    gradient.append(svgElement('stop', {offset: '0%', class: `gradient-${tone}-start`}));
    gradient.append(svgElement('stop', {offset: '100%', class: `gradient-${tone}-end`}));
    defs.append(gradient);
    svg.append(defs);
    svg.append(svgElement('path', {d: areaPath, fill: `url(#${gradientId})`, class: 'spark-area'}));
    svg.append(svgElement('path', {d: linePath, class: `spark-line ${tone}`}));
  }

  async function poll() {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 4000);
    try {
      const response = await fetch(`${API_URL}?t=${Date.now()}`, {
        cache: 'no-store',
        headers: {'Accept': 'application/json'},
        signal: controller.signal
      });
      if (!response.ok) throw new Error(`Agent API HTTP ${response.status}`);
      const snapshot = await response.json();
      state.failures = 0;
      render(snapshot);
    } catch (error) {
      state.failures += 1;
      renderLiveStatus(false, error.name === 'AbortError' ? 'Agent yanıt zaman aşımına uğradı.' : error.message);
    } finally {
      window.clearTimeout(timeout);
      scheduleNext();
    }
  }

  function scheduleNext() {
    window.clearTimeout(state.timer);
    const base = document.hidden ? 5000 : 1500;
    const retryDelay = state.failures ? Math.min(10000, base * Math.pow(1.7, state.failures)) : base;
    state.timer = window.setTimeout(poll, retryDelay);
  }

  document.addEventListener('visibilitychange', () => {
    window.clearTimeout(state.timer);
    scheduleNext();
  });

  poll();
})();
