# Plan: DuckCluster Monitoring Dashboard

**Status:** In progress — P0–P3 implemented (MVP shipped). P4–P5 pending.

**Goal:** A minimal, read-only web UI for cluster health and query activity.  
**Non-goal:** Submitting SQL, editing config, or cluster administration.

**Open dashboard:** `http://<coordinator-host>:<http-port>/dashboard/index.html` (root `/` redirects there).

**Remote access:** HTTP binds to `0.0.0.0` by default (`DUCKCLUSTER_COORDINATOR_HTTP_BIND_HOST`). Workers still connect via `DUCKCLUSTER_COORDINATOR_HOST` (default `127.0.0.1` on the same machine). Restart the coordinator after changing bind settings.

---

## Implementation progress

| Phase | Status | Notes |
|-------|--------|-------|
| **P0** | ✅ Done | `GET /v1/monitor/summary` — cluster, workers, coordinator info |
| **P1** | ✅ Done | Static dashboard at `/dashboard/` with cluster graphics |
| **P2** | ✅ Done | `QueryActivityTracker` + active/recent queries in summary |
| **P3** | ✅ Done | `GET /v1/monitor/shards` — table/shard owners & cache |
| **P4** | ⬜ Pending | SSE live updates (`GET /v1/events`) |
| **P5** | ⬜ Pending | Per-query drill-down + worker cache stats |

### Cluster graphics (P1 enhancement)

The dashboard includes **read-only visuals for coordinator + workers only** (no query-result charts):

- **Hub-and-spoke topology** — coordinator center, workers on a ring; animated links & pulse on healthy nodes
- **Health donut** — healthy vs total workers (teal/violet gradient)
- **Load bars** — per-worker reported load
- **Coordinator card** — HTTP/gRPC endpoints

Palette matches benchmark plots: soft teal + violet on dark slate.

---

## 1. Principles

| Principle | Rationale |
|-----------|-----------|
| **Read-only** | Operators observe; clients keep using REST/`duckcluster` CLI |
| **Minimal** | Tables + status badges first; cluster graphics for at-a-glance ops |
| **Coordinator-served** | Single static page + JSON APIs from coordinator HTTP port |
| **No new dependencies required for v1** | Plain HTML/CSS/vanilla JS; optional SSE later |
| **Low overhead** | Polling every 3–5 s; no per-row streaming from workers |

---

## 2. User stories

1. As an operator, I see whether the cluster is **UP** or **DEGRADED** at a glance. ✅
2. I see each **worker** (id, host:port, heartbeat age, reported load, status). ✅
3. I see **in-flight queries** (id, phase, elapsed time) without tailing logs. ✅
4. I see **recent completed queries** (duration, merge strategy, OK/error). ✅
5. I can confirm **shard coverage** (which tables/shards are owned vs cached) when debugging routing. ✅

---

## 3. What exists today

| Endpoint | Data |
|----------|------|
| `GET /v1/cluster/health` | `status` (UP/DEGRADED), `workerCount` |
| `GET /v1/cluster/workers` | Per worker: id, host, port, threads, status, registeredAt, lastHeartbeatAt, load |
| `GET /v1/monitor/summary` | Aggregated snapshot (health + workers + coordinator + active + recent) |
| `GET /v1/monitor/shards` | Shard catalog: owners, cached workers per table/shard |
| `GET /dashboard/` | Static monitor UI |
| `POST /v1/query` | Returns stats **after** query completes (not used by dashboard) |

---

## 4. UI layout (implemented)

```
┌──────────────────────────────────────────────────────────────────┐
│  DuckCluster Monitor          ● UP     3/3 workers    refreshed 2s │
├──────────────────────────────────────────────────────────────────┤
│  CLUSTER TOPOLOGY (SVG hub-spoke)  │  Health donut + load bars   │
│  coordinator ○─── worker nodes   │  Coordinator HTTP/gRPC card │
├──────────────────────────────────────────────────────────────────┤
│  WORKERS (table)                                                 │
├───────────────────────────────┬──────────────────────────────────┤
│  ACTIVE QUERIES               │  RECENT QUERIES                  │
├───────────────────────────────┴──────────────────────────────────┤
│  SHARDS (chips + expandable per-shard detail)                    │
└──────────────────────────────────────────────────────────────────┘
```

---

## 5. API additions

### Phase 1 — MVP ✅

| Method | Path | Response |
|--------|------|----------|
| `GET` | `/dashboard/` | Static HTML entry |
| `GET` | `/v1/monitor/summary` | Aggregated snapshot |

### Phase 2 — Deeper ops

| Method | Path | Purpose | Status |
|--------|------|---------|--------|
| `GET` | `/v1/monitor/shards` | Shard catalog | ✅ Done |
| `GET` | `/v1/monitor/queries/{id}` | Single query timeline | ⬜ P5 |
| `GET` | `/v1/events` (SSE) | Push updates | ⬜ P4 |

### Phase 3 — Worker pull (optional)

| Method | Path | Purpose |
|--------|------|---------|
| gRPC or `GET /v1/workers/{id}/cache` | Per-worker cache size, pin depth, materialized broadcast tables |

---

## 6. Coordinator instrumentation ✅

| Component | Path | Responsibility |
|-----------|------|----------------|
| `QueryActivityTracker` | `coordinator/.../monitor/QueryActivityTracker.java` | Active queries + ring buffer of 50 recent |
| `MonitorService` | `coordinator/.../monitor/MonitorService.java` | Summary + shards JSON |
| Hooks in `QueryExecutionService.execute()` | Phase transitions PLAN → PREFETCH → FRAGMENTS → MERGE |

---

## 7. Frontend ✅

| Piece | Path |
|-------|------|
| HTML | `coordinator/src/main/resources/dashboard/index.html` |
| CSS | `coordinator/src/main/resources/dashboard/dashboard.css` |
| JS | `coordinator/src/main/resources/dashboard/dashboard.js` |
| Refresh | Summary 3s, shards 12s |

---

## 8. Success criteria

- [x] Dashboard loads from coordinator root URL without separate build step
- [x] Worker stale heartbeat visible within one poll interval
- [x] Active query appears within 500 ms of `POST /v1/query` start
- [x] Completed query appears in recent list with duration and merge strategy
- [x] No SQL execution path from dashboard (no form POST to `/v1/query`)
- [x] Cluster topology graphics for coordinator + workers

---

## 9. Out of scope

- Query editor or result grid
- Benchmark result charts (stay in `docs/plots/` + `BENCHMARK.md`)
- Authentication / multi-tenant
- Worker log tailing
- Prometheus/Grafana export (future; could add `/v1/metrics` Prometheus format in P4+)

---

## 10. Files touched

| Area | Paths |
|------|-------|
| HTTP routes | `coordinator/.../http/CoordinatorHttpServer.java` |
| Monitor API | `coordinator/.../monitor/MonitorService.java` |
| Activity tracking | `coordinator/.../monitor/QueryActivityTracker.java` |
| Execution hooks | `coordinator/.../execution/QueryExecutionService.java` |
| Wiring | `coordinator/.../CoordinatorMain.java` |
| Static assets | `coordinator/src/main/resources/dashboard/*` |
| Tests | `coordinator/.../monitor/QueryActivityTrackerTest.java` |

---

## Next steps

1. **P4** — SSE `/v1/events` to reduce polling overhead for active queries
2. **P5** — `GET /v1/monitor/queries/{id}` timeline + optional worker cache pull
3. Document dashboard in README; delete this plan file when P4/P5 are done or explicitly deferred

---

*Last updated: 2026-07-12 — P0–P3 + cluster graphics shipped.*
