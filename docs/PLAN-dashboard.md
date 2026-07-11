# Plan: DuckCluster Monitoring Dashboard

**Status:** MVP shipped (P0–P3). Open at `http://<coordinator>:<http-port>/dashboard/` (root `/` redirects).

**Demo:** [DuckCluster-demo.mp4](media/DuckCluster-demo.mp4) · Screenshots in [README](../README.md#monitoring-dashboard)

## Delivered

| Phase | What |
|-------|------|
| P0 | `GET /v1/monitor/summary` |
| P1 | Static dashboard — topology SVG, health donut, load bars |
| P2 | `QueryActivityTracker` — active + recent queries |
| P3 | `GET /v1/monitor/shards` |

## TODO

- [ ] **P4** — SSE `GET /v1/events` (replace polling for active queries)
- [ ] **P5** — `GET /v1/monitor/queries/{id}` drill-down + optional worker cache stats
- [ ] Prometheus `/v1/metrics` export (optional)

## Non-goals

SQL editor, result grid, auth, benchmark charts (those live in `docs/BENCHMARK.md`).
