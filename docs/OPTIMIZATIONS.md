# DuckCluster Optimizations

Single index of performance work: what was broken, what we changed, and the benchmark
numbers that prove it. Full run tables and methodology live in [BENCHMARK.md](BENCHMARK.md);
engineering detail in [DESIGN-DECISIONS.md §14](DESIGN-DECISIONS.md#14-challenges--solutions).

**Workload:** TPC-H, 22 queries, concurrency=1 unless noted.  
**Hardware:** Single Linux VM — 3 workers + coordinator vs single-node DuckDB baseline (same machine).

---

## Suite trajectory (canonical milestones)

Exec p50 sum across 22 queries, DuckCluster vs single-node ratio in parentheses.

| Milestone | Run ID | Suite (DC) | vs SN | What landed |
|-----------|--------|------------|-------|-------------|
| Pre-fix | — | minutes+ (Q13/Q16 stall) | — | Row-by-row coordinator INSERT |
| Bulk-load | `sf1-20260710-232214` | **19.2 s** | **9.0×** | `DuckDbBulkInserter` (Appender) |
| Merge-hardening | `sf1-20260711-001805` | **15.5 s** | **7.2×** | CTAS handoff, streaming Appender, coordinator pool |
| Merge pushdown | `sf1-20260711-004753` | **6.6 s** | **3.2×** | Hierarchical merge + TOP-K oversample |
| SF10 pre-pin | `sf10-20260711-000816` | 130.9 s | 4.7× | 20/22 OK (Q10/Q18 cache eviction) |
| SF10 post-pin | `sf10-20260711-024011` + `031257` | 161.9 s / 103.3 s | **1.6×**† | Shard pin session + cache sizing |
| SF20 | `sf20-20260711-042835` | 252 s / 66 s | **3.8×** | Fair `ENGINE=both` pass |
| SF40 | `sf40-20260711-051834` | 396 s / 156 s | **2.5×** | 12 shards; **7/22 DC wins** |

†SF10 single-node baseline run separately; may have been cgroup-contended. SF20/SF40 used
`ENGINE=both` for a cleaner paired comparison.

### Cross-scale headline

| Scale | Suite ratio (DC/SN) | DC faster (of 22) | Notes |
|-------|---------------------|-------------------|-------|
| SF0.01 | ~67× | 0 | Coordination dominates tiny data |
| SF1 (pushdown) | 3.2× | 2+ | Merge path healthy |
| SF10 (post-pin) | 1.6×† | 11† | Best paired DC run |
| SF20 | 3.8× | 2 (Q01, Q04) | Scan winners only |
| SF40 | **2.5×** | **7** | Q01/Q04 at **0.40×** |

---

## Resolved optimizations

### 1. Coordinator bulk load (`DuckDbBulkInserter`)

| | |
|---|---|
| **Problem** | Row-by-row `INSERT INTO __merge_temp VALUES (…)` — O(rows) SQL round-trips; Q13/Q16 stuck **minutes** at SF1 |
| **Solution** | DuckDB `Appender` API; fallback batched INSERT (512 rows) |
| **Key files** | `coordinator/.../DuckDbBulkInserter.java` |
| **Benchmark** | Micro: 10k rows Appender ~40 ms vs batched ~957 ms (~24×). Q13: minutes → **1226 ms**. Suite SF1: enables first full 22/22 pass at **19.2 s** (`sf1-20260710-232214`) |

---

### 2. CTAS phase-1 handoff (no Java ping-pong)

| | |
|---|---|
| **Problem** | `NESTED_GROUP_BY_MERGE` / `WITH_CTE_MERGE` read phase-1 into Java heap, dropped temp table, re-inserted |
| **Solution** | `CREATE TABLE __merge_phase1 AS (…)` then `ALTER … RENAME TO __merge_temp` — data stays in coordinator DuckDB |
| **Key files** | `coordinator/.../DuckDbMergeSupport.materializePhase1InDb` |
| **Benchmark** | Q15: 210 ms → **136 ms** (−35%). Q13: 1226 ms → **1076 ms** (−12%). Part of hardening **15.5 s** suite |

---

### 3. Fragment streaming to Appender

| | |
|---|---|
| **Problem** | Proto → `RowBatchData` → `List<List<Object>>` before merge insert; 3–4 heap copies per fragment |
| **Solution** | `appendFragmentBatches()` writes each gRPC batch directly into Appender |
| **Key files** | `DuckDbBulkInserter.appendFragmentBatches` |
| **Benchmark** | Q16: 699 ms → **385 ms** (−45%). Lower coordinator heap across all merge queries |

---

### 4. Coordinator DuckDB connection pool

| | |
|---|---|
| **Problem** | New in-memory DuckDB per merge; unbounded instances under concurrent clients |
| **Solution** | `CoordinatorDuckDbPool` — bounded queue (size 4), `resetCatalog()` on check-in |
| **Key files** | `coordinator/.../CoordinatorDuckDbPool.java` |
| **Benchmark** | Prevents OOM at c=4; latency still **2.5× worse** than c=1 (`sf1-20260710-234611`) — pool bounds resources, does not parallelize merge |

---

### 5. Merge pushdown (hierarchical + TOP-K oversample)

| | |
|---|---|
| **Problem** | All fragment partials loaded into one `__merge_temp`; global GROUP BY over millions of rows (Q03/Q07 worst) |
| **Solution** | **Hierarchical merge:** collapse multi-fragment workers with intermediate GROUP BY before final merge. **TOP-K oversample:** fragments emit `ORDER BY … LIMIT (limit × shard_count)` when safe |
| **Key files** | `MergePushdownPlanner.java`, `MergeSqlBuilder.java`, `DuckDbMergeSupport.mergeGroupByHierarchical` |
| **Benchmark** | Suite SF1: **15.5 s → 6.6 s** (−57%). Q03: 2306 ms → **226 ms** (−90%). Q07: 2926 ms → **295 ms** (−90%). Ratio: **7.2× → 3.2×** (`sf1-20260711-004753`) |

---

### 6. Worker broadcast cache pin session

| | |
|---|---|
| **Problem** | LRU `TempShardCache` evicted `orders_shard0` mid-fragment → `Catalog Error: … does not exist` (SF10 Q10/Q18) |
| **Solution** | `BeginShardPin` / `EndShardPin` gRPC; pinned keys skip eviction; larger default cache (`DUCKCLUSTER_CACHE_MAX_SHARDS=64` in benchmarks) |
| **Key files** | `TempShardCache.java`, `WorkerGrpcServer.java`, `QueryExecutionService.java`, `cluster.proto` |
| **Benchmark** | SF10: **20/22 → 22/22 OK**. Stable through SF40 (56 GB, 12 shards). See [DESIGN-DECISIONS.md §6](DESIGN-DECISIONS.md#6-fragment-routing-prefetch-and-waiting) |

---

### 7. Planner hardening (22/22 TPC-H correctness)

| | |
|---|---|
| **Problem** | 0/17 queries on first SF0.01 smoke — planner tested only on simple `events` table |
| **Solution** | Expression aggs, derived tables, comma-joins, nested arithmetic, COUNT(DISTINCT), correlated subqueries, WITH CTE merge, TOP_K on joins — one pipeline, no alternate modes |
| **Key files** | `CalciteQueryPlanner.java`, `FragmentSqlGenerator.java`, `MergeSqlBuilder.java`, integration tests |
| **Benchmark** | **22/22 PASS** vs `baseline.duckdb` at SF0.01 (`test_tpch.py`). Prerequisite for all performance numbers above |
| **Detail** | [DESIGN-planner.md](DESIGN-planner.md), [DESIGN-DECISIONS.md §14 Challenge 7](DESIGN-DECISIONS.md#challenge-7-planner-correctness--022-to-2222-tpc-h-queries) |

---

### 8. Broadcast materialization (worker-side `__dc_bcast_*`)

| | |
|---|---|
| **Problem** | Every fragment SQL contained `UNION ALL` over all broadcast shards — repeated scans per fragment |
| **Solution** | Materialize each multi-shard broadcast once per worker per query as `__dc_bcast_<table>`; `MaterializeBroadcast` / `ClearBroadcastTables` RPC; planner references materialized table in fragment SQL |
| **Key files** | `BroadcastMaterializer.java`, `BroadcastSqlNames.java`, `FragmentSqlGenerator.java`, `QueryExecutionService.prepareWorkersForQuery` |
| **Benchmark** | **Pending formal re-benchmark** — code merged; canonical SF10/SF20/SF40 numbers predate or overlap this work. Expected to help Q03/Q07/Q08/Q09/Q18 |

---

### 9. Per-query worker preparation + hierarchical CTAS (coordinator)

| | |
|---|---|
| **Problem** | Pin/materialize per fragment attempt; hierarchical collapse round-tripped merge rows through Java strings |
| **Solution** | `prepareWorkersForQuery()` — one pin + broadcast materialize per worker per query; hierarchical step uses `CREATE TABLE __merge_worker_N AS` + `INSERT INTO __merge_temp SELECT *` |
| **Key files** | `QueryExecutionService.java`, `DuckDbMergeSupport.collapseWorkerFragments` |
| **Benchmark** | **Pending formal re-benchmark**. Reduces RPC/pin churn and coordinator string materialization |

---

### 10. Operational / harness fixes (not query-path latency)

| Fix | Impact |
|-----|--------|
| Default log level INFO (was DEBUG) | SF10 run generated ~19 GB logs; fixed in `logback.xml` + `benchmark-run.sh` |
| `benchmark-cleanup.sh` / `--clean-other-sf` | Disk headroom before large-SF datagen |
| `benchmark-run.sh` `--sf` flag parsing | Harness accepts `--sf 10` correctly |
| Warmup progress in `BenchmarkRunner` | Visible harness phase during long runs |

---

## Per-optimization query movement (SF1, c=1)

Selected p50 exec ms. Hardening run: `sf1-20260711-001805`. Pushdown run: `sf1-20260711-004753`.

| Query | Bulk-load | Hardening | Pushdown | Merge strategy |
|-------|-----------|-----------|----------|----------------|
| Q03 | 2306+ | 2306 | **226** | GROUP_BY_MERGE + TOP-K |
| Q07 | 4534 | 2926 | **295** | GROUP_BY_MERGE + TOP-K |
| Q13 | 1226 | 1076 | — | NESTED_GROUP_BY_MERGE |
| Q15 | 210 | **136** | — | WITH_CTE_MERGE |
| Q16 | 699 | **385** | — | GROUP_BY_MERGE |
| Q21 | 1237 | **752** | — | GROUP_BY_MERGE |

---

## SF40 — where optimizations show up

Run: `sf40-20260711-051834` (warmup=2, iter=3, 12 shards, ~56 GB).

| Category | Queries | DC/SN ratio | Why |
|----------|---------|-------------|-----|
| **DC wins** | Q01, Q02, Q04, Q06, Q11, Q20, Q22 | 0.40×–0.93× | Parallel scan; trivial or local merge |
| **Near parity** | Q19, Q21 | 1.07×, 1.11× | Next to cross over |
| **Still slow** | Q03, Q07, Q08, Q09, Q18 | 3.1×–5.0× | Broadcast + coordinator merge dominate |

**Scaling:** DC suite **1.57×** for 2× data (SF20→SF40); SN **2.36×** for 2× data.

---

## Open bottlenecks

Ordered by measured impact at SF10–SF40. See [BENCHMARK.md — Remaining bottlenecks](BENCHMARK.md#remaining-bottlenecks).

| # | Bottleneck | Symptom | Status |
|---|------------|---------|--------|
| 1 | Broadcast cost (even with materialization at scale) | Q03/Q07/Q08/Q09/Q18 remain 3–5× slower than SN | **Partial** — materialization landed; full suite re-benchmark pending |
| 2 | Coordinator merge SQL | Workers finish in &lt;1 s; Q03 still multi-second at SF10+ | **Partial** — pushdown helped SF1; large fan-in remains |
| 3 | Result materialization | Client latency &gt; exec time (e.g. Q13) | **Open** — full `ResultSet` → JSON, no streaming |
| 4 | gRPC fragment overhead | ~50–200 ms fixed tax per query | **Open** |
| 5 | Coordinator merge serialization | c=4 suite **2.5× worse** than c=1 | **Open** — needs parallel merge executor |
| 6 | All-string proto columns | Parse/convert overhead at merge | **Open** — Arrow/typed cells deferred |
| 7 | Fragment wave count | 6–12 fragments on 3 workers → 2–4 execution waves | **Open** — more workers/shards |
| 8 | Node stability | Workers crash / SSH hangs at sustained SF40+ load | **Open** — blocks SF100 |

---

## Recommended next optimizations

| Priority | Work | Queries | Expected impact |
|----------|------|---------|-----------------|
| 1 | Re-benchmark after broadcast materialization + per-query pin | Q03/Q07/Q08/Q09/Q18 | Quantify #8 above |
| 2 | Stream results (chunked HTTP or Arrow) | Q13, Q18, large outputs | Cut client–exec gap |
| 3 | Parallel merge executor + backpressure | All queries at c≥4 | Throughput crossover |
| 4 | Typed proto / Arrow worker → coordinator | Merge-heavy queries | Less parse CPU at SF10+ |
| 5 | More workers + shards | Scan-heavy Q01/Q04/Q06 | Hide RPC latency |

---

## Related docs

| Document | Contents |
|----------|----------|
| [BENCHMARK.md](BENCHMARK.md) | Full per-query tables, run log, success criteria, harness reference |
| [DESIGN-DECISIONS.md §14](DESIGN-DECISIONS.md#14-challenges--solutions) | Problem/solution narratives with code snippets |
| [DESIGN-planner.md](DESIGN-planner.md) | Planner pipeline and TPC-H capability map |
| [BENCHMARK-COMMANDS.md](BENCHMARK-COMMANDS.md) | How to reproduce runs |

*Delete or archive this file once optimizations are fully captured in release notes; until then it is the canonical optimization + numbers index.*
