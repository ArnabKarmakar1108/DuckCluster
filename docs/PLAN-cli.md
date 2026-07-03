# Plan: DuckCluster CLI

**Status:** Planning only — not implemented. Delete this file once the CLI ships.

**Goal:** Replace manual `curl` + `jq` with a small command-line client for queries and cluster inspection.  
**Non-goal:** Cluster administration, datagen, benchmark orchestration (stay in `scripts/`).

---

## 1. Why a CLI

| Today (`curl`) | Pain |
|----------------|------|
| `curl -X POST … -d '{"sql":"…"}'` | SQL quoting in shell |
| Raw JSON output | Hard to read ad-hoc results |
| No standard exit codes | Scripts must parse HTTP status |
| Discoverability | Users read README for endpoints |

A CLI improves **daily dev**, **integration debugging**, and **CI smoke tests** without changing the coordinator API.

---

## 2. Design principles

| Principle | Choice |
|-----------|--------|
| **Thin client** | All work stays on coordinator REST API — no gRPC to workers from CLI |
| **Script-friendly** | `--format json` default for piping; `--format table` for humans |
| **Configurable endpoint** | `--url` flag + `DUCKCLUSTER_URL` env (default `http://127.0.0.1:8080`) |
| **Minimal surface** | v1: three commands only |
| **Reuse patterns** | Align with `tests/integration/duckcluster/` HTTP client |

---

## 3. Proposed command tree (v1)

```
duckcluster
├── query <sql>              Run SQL synchronously
├── query -f <file>          Run SQL from file
├── status                   Cluster health summary
└── workers                  List registered workers
```

### Global flags

```
--url <base>       Coordinator base URL (default http://127.0.0.1:8080)
--timeout <sec>    HTTP timeout (default 300)
--format <fmt>     json | table (default: table for TTY, json if stdout piped)
-v, --verbose      Print query stats (duration, merge strategy, workers)
```

---

## 4. Command specifications

### 4.1 `duckcluster query`

**Maps to:** `POST /v1/query`

```bash
# Interactive
duckcluster query "SELECT category, COUNT(*) AS cnt FROM events GROUP BY category"

# From file
duckcluster query -f report.sql

# Scripting
duckcluster query "SELECT 1" --format json | jq '.rows'

# With stats on stderr
duckcluster query "SELECT …" -v
# stderr: merge=GROUP_BY_MERGE duration=120ms workers=3 fragments=6
```

**Exit codes:**

| Code | Meaning |
|------|---------|
| 0 | Query OK, rows returned (or empty result) |
| 1 | HTTP 4xx/5xx or coordinator error JSON |
| 2 | Local error (bad args, connection refused, timeout) |

**Table format (human):**

- Print column headers + aligned rows (cap width at terminal columns)
- Truncate cell display at 40 chars with `…`
- Row count footer: `(N rows in 120 ms)`

**JSON format:** Pass through coordinator response body unchanged:

```json
{
  "queryId": "…",
  "columns": ["category", "cnt"],
  "rows": [["A", 3]],
  "stats": { "mergeStrategy": "GROUP_BY_MERGE", "durationMs": 120, … }
}
```

---

### 4.2 `duckcluster status`

**Maps to:** `GET /v1/cluster/health` (+ optional worker count from `/v1/cluster/workers`)

```bash
$ duckcluster status
Cluster: UP
Workers: 3 registered (3 healthy)
Coordinator: http://127.0.0.1:8080
```

`--format json`:

```json
{ "status": "UP", "workerCount": 3, "healthyWorkers": 3 }
```

Exit 0 if UP, 1 if DEGRADED, 2 if unreachable.

---

### 4.3 `duckcluster workers`

**Maps to:** `GET /v1/cluster/workers`

```bash
$ duckcluster workers
WORKER     HOST         PORT   STATUS    HEARTBEAT   LOAD
worker-1   127.0.0.1    9101   HEALTHY   2s ago      0.12
worker-2   127.0.0.1    9102   HEALTHY   1s ago      0.08
```

`--format json`: pass through `{ "workers": [ … ] }`.

---

## 5. Implementation options

| Option | Pros | Cons | Recommendation |
|--------|------|------|----------------|
| **A. Python package** (`cli/` or `scripts/duckcluster`) | Fast; reuse `requests`; matches integration tests | Separate from Java artifacts | **Preferred for v1** |
| **B. Java picocli module** | Single Maven module; shaded JAR with coordinator | More boilerplate; slower iteration | v2 if one-binary distribution matters |
| **C. Shell wrapper on curl** | Zero deps | Quoting still painful; weak table output | Not recommended |

### Recommended: Python v1

```
cli/
├── pyproject.toml          # optional; or requirements-cli.txt
├── duckcluster/
│   ├── __main__.py         # entry: python -m duckcluster
│   ├── client.py           # HTTP wrapper
│   ├── commands.py         # query | status | workers
│   └── format.py           # table rendering
└── README.md
```

**Install (dev):**

```bash
pip install -e cli/
duckcluster status
```

**Or** ship as `scripts/duckcluster` single file (~200 lines) with stdlib `urllib` only — no pip dep.

---

## 6. Configuration

| Source | Precedence |
|--------|------------|
| CLI flags | Highest |
| `DUCKCLUSTER_URL` | Override default base URL |
| `DUCKCLUSTER_TIMEOUT` | Optional timeout override |
| Default `http://127.0.0.1:8080` | Lowest |

No config file for v1.

---

## 7. Phased delivery

| Phase | Scope | Deliverable |
|-------|-------|-------------|
| **P0** | `query --format json` only | Scriptable minimum |
| **P1** | `status`, `workers`, table output | Daily dev UX |
| **P2** | `-f`, `-v`, exit codes, timeout | Production scripting |
| **P3** | Shell completion, `duckcluster query --explain` (if API added) | Nice-to-have |

**MVP:** P0 + P1 (three commands, json + table).

---

## 8. Testing plan (when implementing)

| Test | Type |
|------|------|
| `duckcluster status` against running cluster from `start-cluster.sh` | Manual + integration script |
| `duckcluster query "SELECT 1"` matches curl response byte-for-byte (json mode) | Automated |
| Exit code 2 when coordinator down | Automated |
| SQL file with multiline query | Manual |
| Unicode / quoted identifiers in SQL | Manual |

Add optional job to CI: start ephemeral cluster → `duckcluster query` smoke (mirror `test_tpch.py` client).

---

## 9. Documentation updates (when implementing)

| File | Change |
|------|--------|
| `README.md` | Replace curl examples with `duckcluster query`; keep curl as “raw API” appendix |
| `docs/BENCHMARK-COMMANDS.md` | Note CLI optional for smoke tests |
| Delete | `docs/PLAN-cli.md` |

---

## 10. Future commands (v2+, not planned now)

| Command | Purpose |
|---------|---------|
| `duckcluster query --watch` | Poll long-running query (needs async API) |
| `duckcluster plan "…"` | Dry-run: show fragments/merge strategy without execute |
| `duckcluster benchmark` | Thin wrapper over `benchmark-run.sh` |
| `duckcluster cluster health --json` | Alias; metrics for monitor dashboard |

Defer until REST API exposes plan-only or async query endpoints.

---

## 11. Non-goals (explicit)

- Starting/stopping coordinator or workers
- Shard upload / `split-and-distribute.sh` integration
- TPC-H datagen
- Authenticated sessions
- REPL mode (DuckDB CLI replacement)

---

## 12. Success criteria

- [ ] User can run TPC-H-style query from file without shell escaping issues
- [ ] `duckcluster status` works against default local cluster
- [ ] JSON output stable enough for `jq` scripts in CI
- [ ] README quick-start uses CLI as primary interface

---

*Remove `docs/PLAN-cli.md` after the CLI is documented in README.*
