const POLL_MS = 3000;
const RECENT_VISIBLE = 5;

let lastRefreshMs = 0;
let shardCache = null;
let shardStale = false;
let recentExpanded = false;

const el = (id) => document.getElementById(id);

function formatAge(ms) {
  if (ms < 1000) return `${ms}ms ago`;
  if (ms < 60_000) return `${Math.round(ms / 1000)}s ago`;
  return `${Math.round(ms / 60_000)}m ago`;
}

function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function workerVisualStatus(worker) {
  if (worker.status !== "HEALTHY") return "unhealthy";
  if (worker.stale) return "stale";
  return "healthy";
}

function statusLabel(worker) {
  const v = workerVisualStatus(worker);
  if (v === "healthy") return "healthy";
  if (v === "stale") return "stale";
  return "unhealthy";
}

function sortedWorkers(workers) {
  return [...workers].sort((a, b) => a.workerId.localeCompare(b.workerId));
}

function renderClusterStatus(data) {
  const cluster = data.cluster;
  const statusEl = el("cluster-status");
  const isUp = cluster.status === "UP";
  statusEl.textContent = cluster.status;
  statusEl.className = `pill ${isUp ? "pill-up" : "pill-degraded"}`;

  el("worker-ratio").textContent =
    `${cluster.healthyWorkers}/${cluster.workerCount} workers healthy`;

  lastRefreshMs = data.refreshedAtEpochMs;
  updateRefreshAge();
}

function updateRefreshAge() {
  if (!lastRefreshMs) return;
  const age = Date.now() - lastRefreshMs;
  el("refresh-age").textContent = `refreshed ${formatAge(age)}`;
}

function workerPosition(index, count, cx, cy, radius) {
  const angle = (2 * Math.PI * index) / count - Math.PI / 2;
  return {
    x: cx + radius * Math.cos(angle),
    y: cy + radius * Math.sin(angle),
    angle,
  };
}

function renderTopology(data) {
  const svg = el("topology-svg");
  const workers = sortedWorkers(data.workers || []);
  const cx = 210;
  const cy = 160;
  const radius = workers.length === 0 ? 0 : Math.min(115, 55 + workers.length * 12);
  const coordColor = data.cluster.status === "UP" ? "#2dd4bf" : "#fbbf24";

  let html = `
    <defs>
      <radialGradient id="coord-grad">
        <stop offset="0%" stop-color="#2dd4bf"/>
        <stop offset="100%" stop-color="#7c3aed"/>
      </radialGradient>
    </defs>
  `;

  workers.forEach((worker, i) => {
    const pos = workerPosition(i, workers.length, cx, cy, radius);
    const visual = workerVisualStatus(worker);
    const linkOpacity = visual === "healthy" ? 0.9 : visual === "stale" ? 0.55 : 0.35;
    const stroke = visual === "healthy" ? "#8b5cf6" : visual === "stale" ? "#fbbf24" : "#f87171";
    const gradId = `link-grad-${i}`;
    html += `
      <linearGradient id="${gradId}" gradientUnits="userSpaceOnUse"
        x1="${cx}" y1="${cy}" x2="${pos.x}" y2="${pos.y}">
        <stop offset="0%" stop-color="#2dd4bf" stop-opacity="0.35"/>
        <stop offset="55%" stop-color="${stroke}" stop-opacity="0.95"/>
        <stop offset="100%" stop-color="#2dd4bf" stop-opacity="0.5"/>
      </linearGradient>
    `;
    const dash = visual === "healthy" ? 'class="topo-link"' : "";
    html += `<line x1="${cx}" y1="${cy}" x2="${pos.x}" y2="${pos.y}"
      stroke="url(#${gradId})" stroke-width="2.5" stroke-linecap="round"
      opacity="${linkOpacity}" ${dash}/>`;
  });

  if (data.cluster.status === "UP" && workers.some((w) => workerVisualStatus(w) === "healthy")) {
    html += `<circle class="topo-pulse" cx="${cx}" cy="${cy}" r="32" fill="none" stroke="${coordColor}" stroke-width="1.5"/>`;
  }

  html += `
    <g class="topo-coord-glow">
      <circle cx="${cx}" cy="${cy}" r="36" fill="url(#coord-grad)"/>
      <text x="${cx}" y="${cy - 6}" text-anchor="middle" class="topo-node-label">COOR</text>
      <text x="${cx}" y="${cy + 6}" text-anchor="middle" class="topo-node-sub topo-node-sub-host">${escapeHtml(data.coordinator.host)}</text>
      <text x="${cx}" y="${cy + 16}" text-anchor="middle" class="topo-node-sub">:${data.coordinator.httpPort}</text>
    </g>
  `;

  workers.forEach((worker, i) => {
    const pos = workerPosition(i, workers.length, cx, cy, radius);
    const visual = workerVisualStatus(worker);
    const fill = visual === "healthy" ? "#34d399" : visual === "stale" ? "#fbbf24" : "#f87171";
    const shortId = worker.workerId.replace(/^worker-/, "w-");

    if (visual === "healthy") {
      html += `<circle class="topo-pulse" cx="${pos.x}" cy="${pos.y}" r="22" fill="none" stroke="${fill}" stroke-width="1" style="animation-delay:${i * 0.3}s"/>`;
    }

    html += `
      <circle cx="${pos.x}" cy="${pos.y}" r="28" fill="${fill}" opacity="0.9"/>
      <text x="${pos.x}" y="${pos.y - 3}" text-anchor="middle" class="topo-node-label">${escapeHtml(shortId)}</text>
      <text x="${pos.x}" y="${pos.y + 9}" text-anchor="middle" class="topo-node-sub">:${worker.port}</text>
    `;
  });

  if (workers.length === 0) {
    html += `<text x="${cx}" y="${cy + 55}" text-anchor="middle" fill="#94a3b8" font-size="12">No workers registered</text>`;
  }

  svg.innerHTML = html;
}

function renderHealthDonut(data) {
  const svg = el("health-donut");
  const total = data.cluster.workerCount || 0;
  const healthy = data.cluster.healthyWorkers || 0;
  const r = 42;
  const c = 2 * Math.PI * r;
  const pct = total === 0 ? 0 : healthy / total;
  const offset = c * (1 - pct);

  svg.innerHTML = `
    <circle cx="60" cy="60" r="${r}" fill="none" stroke="#334155" stroke-width="12"/>
    <circle cx="60" cy="60" r="${r}" fill="none" stroke="url(#donut-grad)" stroke-width="12"
      stroke-dasharray="${c}" stroke-dashoffset="${offset}"
      transform="rotate(-90 60 60)" stroke-linecap="round"/>
    <defs>
      <linearGradient id="donut-grad" x1="0" y1="0" x2="1" y2="1">
        <stop offset="0%" stop-color="#2dd4bf"/>
        <stop offset="100%" stop-color="#8b5cf6"/>
      </linearGradient>
    </defs>
    <text x="60" y="56" text-anchor="middle" fill="#e2e8f0" font-size="18" font-weight="600">${healthy}</text>
    <text x="60" y="72" text-anchor="middle" fill="#94a3b8" font-size="10">/ ${total}</text>
  `;

  el("health-caption").textContent =
    total === 0 ? "Waiting for workers" : `${Math.round(pct * 100)}% healthy`;
}

function renderLoadBars(workers) {
  const container = el("load-bars");
  const sorted = sortedWorkers(workers);
  if (!sorted.length) {
    container.innerHTML = '<span class="empty-state">No workers</span>';
    return;
  }

  const maxLoad = Math.max(0.01, ...sorted.map((w) => w.load || 0));
  container.innerHTML = sorted.map((w) => {
    const pct = Math.min(100, ((w.load || 0) / maxLoad) * 100);
    const label = w.workerId.length > 8 ? w.workerId.slice(0, 7) + "…" : w.workerId;
    return `
      <div class="load-row">
        <span class="load-label" title="${w.workerId}">${label}</span>
        <div class="load-track"><div class="load-fill" style="width:${pct}%"></div></div>
        <span class="load-value">${(w.load || 0).toFixed(2)}</span>
      </div>
    `;
  }).join("");
}

function renderCoordinatorInfo(coord) {
  el("coordinator-info").innerHTML = `
    <dt>HTTP</dt><dd>${coord.host}:${coord.httpPort}</dd>
    <dt>gRPC</dt><dd>${coord.host}:${coord.grpcPort}</dd>
  `;
}

function renderWorkersTable(workers) {
  const tbody = el("workers-body");
  const sorted = sortedWorkers(workers);
  if (!sorted.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No workers registered</td></tr>';
    return;
  }

  tbody.innerHTML = sorted.map((w) => {
    const visual = workerVisualStatus(w);
    return `
      <tr>
        <td class="mono">${w.workerId}</td>
        <td class="mono">${w.host}:${w.port}</td>
        <td><span class="status-dot status-${visual}">${statusLabel(w)}</span></td>
        <td>${formatAge(w.lastHeartbeatAgeMs)}</td>
        <td class="mono">${(w.load != null ? w.load : 0).toFixed(2)}</td>
        <td>${w.numThreads}</td>
      </tr>
    `;
  }).join("");
}

function renderActiveQueries(queries) {
  el("active-count").textContent = String(queries.length);
  const container = el("active-queries");

  if (!queries.length) {
    container.className = "query-list empty-state";
    container.textContent = "No queries running";
    return;
  }

  container.className = "query-list";
  container.innerHTML = queries.map((q) => {
    const progress = q.fragmentsTotal > 0
      ? Math.round((q.fragmentsDone / q.fragmentsTotal) * 100)
      : phaseProgress(q.phase);
    return `
      <article class="query-card">
        <header>
          <span class="query-id">${shortId(q.queryId)}</span>
          <span class="query-meta">${q.phase} · ${formatDuration(q.elapsedMs)}</span>
        </header>
        <p class="query-sql" title="${escapeHtml(q.sqlPreview)}">${escapeHtml(q.sqlPreview)}</p>
        <div class="phase-bar"><div class="phase-fill" style="width:${progress}%"></div></div>
        ${q.fragmentsTotal > 0 ? `<span class="query-meta">fragments ${q.fragmentsDone}/${q.fragmentsTotal}</span>` : ""}
      </article>
    `;
  }).join("");
}

function phaseProgress(phase) {
  const map = { PLAN: 10, PREFETCH: 30, FRAGMENTS: 60, MERGE: 90, DONE: 100 };
  return map[phase] || 0;
}

function queryCardHtml(q) {
  return `
    <article class="query-card">
      <header>
        <span class="query-id">${shortId(q.queryId)}</span>
        <span class="query-meta badge-${q.status === "OK" ? "ok" : "error"}">
          ${q.mergeStrategy} · ${formatDuration(q.durationMs)} · ${q.status}
        </span>
      </header>
      <p class="query-sql" title="${escapeHtml(q.sqlPreview)}">${escapeHtml(q.sqlPreview)}</p>
    </article>
  `;
}

function renderRecentQueries(queries) {
  const container = el("recent-queries");
  const toggle = el("recent-toggle");

  if (!queries.length) {
    container.className = "query-list empty-state";
    container.textContent = "No completed queries yet";
    toggle.classList.add("hidden");
    return;
  }

  const visible = recentExpanded ? queries : queries.slice(0, RECENT_VISIBLE);
  const hiddenCount = queries.length - RECENT_VISIBLE;

  container.className = "query-list";
  container.innerHTML = visible.map(queryCardHtml).join("");

  if (queries.length > RECENT_VISIBLE) {
    toggle.classList.remove("hidden");
    toggle.textContent = recentExpanded
      ? "Show less"
      : `Show ${hiddenCount} more`;
  } else {
    toggle.classList.add("hidden");
  }
}

function renderWorkerTags(workers, label) {
  if (!workers || !workers.length) {
    return '<span class="shard-none">—</span>';
  }
  return workers.map((w) => `<span class="shard-worker-tag">${escapeHtml(w)}</span>`).join("");
}

function renderShardSummary(shards) {
  const summary = el("shard-summary");
  const staleEl = el("shard-stale");
  const tables = shards.tables || [];

  staleEl.classList.toggle("hidden", !shardStale);

  if (!tables.length) {
    summary.className = "shard-summary empty-state";
    summary.textContent = shardStale && shardCache
      ? "Shard catalog empty (showing last snapshot)"
      : "No sharded tables registered yet";
    el("shard-detail").innerHTML = "";
    return;
  }

  const totalShards = tables.reduce((n, t) => n + (t.shardCount || 0), 0);
  summary.className = "shard-summary";
  let chips = `<span class="shard-chip shard-chip-total"><strong>${tables.length}</strong> tables · <strong>${totalShards}</strong> shards</span>`;
  chips += tables.map((t) =>
    `<span class="shard-chip"><strong>${escapeHtml(t.tableName)}</strong> ${t.shardCount}</span>`
  ).join("");

  const under = shards.underReplicated || [];
  if (under.length) {
    chips += `<span class="shard-chip warn-chip">⚠ ${under.length} under-replicated</span>`;
  }

  summary.innerHTML = chips;

  const detail = tables.map((t) => {
    const rows = (t.shards || []).map((s) => `
      <tr>
        <td class="mono">${s.shardId}</td>
        <td>${renderWorkerTags(s.owners, "owners")}</td>
        <td>${renderWorkerTags(s.cachedWorkers, "cache")}</td>
      </tr>
    `).join("");
    return `
      <div class="shard-table-block">
        <h3 class="shard-table-name">${escapeHtml(t.tableName)} <span class="shard-table-count">${t.shardCount} shards</span></h3>
        <div class="table-wrap">
          <table class="shard-table">
            <thead>
              <tr><th>Shard</th><th>Owners</th><th>Cached</th></tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
      </div>
    `;
  }).join("");
  el("shard-detail").innerHTML = detail;
}

function shortId(id) {
  return id.length > 12 ? id.slice(0, 8) + "…" + id.slice(-4) : id;
}

function escapeHtml(text) {
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function showError(message) {
  const banner = el("error-banner");
  banner.textContent = message;
  banner.classList.remove("hidden");
}

function hideError() {
  el("error-banner").classList.add("hidden");
}

async function fetchSummary() {
  const res = await fetch("/v1/monitor/summary");
  if (!res.ok) throw new Error(`Summary API ${res.status}`);
  return res.json();
}

async function fetchShards() {
  const res = await fetch("/v1/monitor/shards");
  if (!res.ok) throw new Error(`Shards API ${res.status}`);
  return res.json();
}

async function refreshShards() {
  try {
    shardCache = await fetchShards();
    shardStale = false;
    renderShardSummary(shardCache);
  } catch (err) {
    shardStale = true;
    if (shardCache) {
      renderShardSummary(shardCache);
    } else {
      el("shard-summary").className = "shard-summary empty-state";
      el("shard-summary").textContent = "Shard catalog unavailable (coordinator may be restarting)";
      el("shard-detail").innerHTML = "";
    }
    el("shard-stale").classList.remove("hidden");
    console.warn("Shard fetch failed:", err.message);
  }
}

async function refresh() {
  try {
    const data = await fetchSummary();
    hideError();
    renderClusterStatus(data);
    renderTopology(data);
    renderHealthDonut(data);
    renderLoadBars(data.workers);
    renderCoordinatorInfo(data.coordinator);
    renderWorkersTable(data.workers);
    renderActiveQueries(data.activeQueries);
    renderRecentQueries(data.recentQueries);
    await refreshShards();
  } catch (err) {
    showError(`Failed to load monitor data: ${err.message}`);
    await refreshShards();
  }
}

el("recent-toggle").addEventListener("click", () => {
  recentExpanded = !recentExpanded;
  refresh();
});

setInterval(updateRefreshAge, 1000);
setInterval(refresh, POLL_MS);
refresh();
