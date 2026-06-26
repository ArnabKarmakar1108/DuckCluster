# DuckCluster Benchmark

This document tracks the TPC-H benchmark program for DuckCluster vs single-node DuckDB.
Benchmark design rationale and harness details are in the Harness reference section below.

**Optimization index:** [OPTIMIZATIONS.md](OPTIMIZATIONS.md) — bottlenecks, fixes, and benchmark numbers in one place.

**Planner status:** All **22/22** TPC-H queries pass SF0.01 correctness (integration tests + fragment
validation). Benchmarking is **unblocked**.

---

## Environment

| Item | Value |
|------|-------|
| Host | Linux VM — **reported** 32 vCPU / 503 GiB RAM; likely **cgroup-limited** (actual CPU/memory may be lower than `nproc` / `/proc/meminfo` suggest) |
| OS | Linux 5.4 (el7) |
| Java | OpenJDK 17.0.18 |
| DuckDB JDBC | 1.1.3 |
| DuckCluster | 0.1.0-SNAPSHOT (merge pushdown + shard-pin fix) |
| Workers | 3 (`worker-1` … `worker-3`) |
| Shards | 6 per partitioned table at SF1; 9 at SF10/SF20; 12 at SF40 (`lineitem`, `orders`) |
| Coordinator | Calcite planner + in-memory DuckDB merge (bulk Appender, hierarchical merge, pool) |
| Worker broadcast cache | `DUCKCLUSTER_CACHE_MAX_SHARDS=64` in benchmarks; pin-session during fragment prefetch |

Treat absolute latencies as **relative** across runs on this host, not as bare-metal SF10 expectations.

---

## Query coverage

All 22 queries are supported and pass correctness on SF0.01.

| Query | Merge strategy | Correctness (SF0.01) | Benchmark (SF0.01 smoke) |
|-------|----------------|----------------------|--------------------------|
| Q01 | PARTIAL_AGG | PASS | OK |
| Q02 | TOP_K | PASS | OK |
| Q03 | TOP_K | PASS | OK |
| Q04 | GROUP_BY_MERGE | PASS | OK |
| Q05 | GROUP_BY_MERGE | PASS | OK |
| Q06 | PARTIAL_AGG | PASS | OK |
| Q07 | CONCATENATE | PASS | OK |
| Q08 | CONCATENATE | PASS | OK |
| Q09 | CONCATENATE | PASS | OK |
| Q10 | GROUP_BY_MERGE | PASS | OK |
| Q11 | GROUP_BY_MERGE | PASS | OK |
| Q12 | PARTIAL_AGG | PASS | OK |
| Q13 | NESTED_GROUP_BY_MERGE | PASS | OK |
| Q14 | PARTIAL_AGG | PASS | OK |
| Q15 | WITH_CTE_MERGE | PASS | OK |
| Q16 | GROUP_BY_MERGE | PASS | OK |
| Q17 | PARTIAL_AGG | PASS | OK |
| Q18 | TOP_K | PASS | OK |
| Q19 | PARTIAL_AGG | PASS | OK |
| Q20 | TOP_K | PASS | OK |
| Q21 | GROUP_BY_MERGE | PASS | OK |
| Q22 | GROUP_BY_MERGE | PASS | OK |

Correctness gate: `tests/integration/test_tpch.py` (all queries vs `baseline.duckdb`).

---

## Setup checklist

- [x] Confirm hardware profile for baseline runs
- [x] Record DuckDB and DuckCluster versions
- [x] Generate TPC-H data for SF0.01
- [x] Verify cluster starts with benchmark worker data
- [x] SF0.01 smoke: 22/22 `status=OK` on both engines (`sf0.01-20260710-204934`)
- [x] Generate TPC-H data for SF1 (`./scripts/benchmark-datagen.sh 1`)
- [x] Generate TPC-H data for SF10 (`PLACEMENT=balanced ./scripts/benchmark-datagen.sh 10 9`)
- [x] Generate TPC-H data for SF40 (`PLACEMENT=balanced ./scripts/benchmark-datagen.sh 40 12`)
- [ ] Generate TPC-H data for SF100 (`./scripts/benchmark-datagen.sh 100`)

Quick smoke command:

```bash
./scripts/benchmark-run.sh 0.01 SKIP_DATAGEN=1 CONCURRENCY=1 WARMUP=1 ITERATIONS=3 ENGINE=both
```

---

## Execution matrix

| Scale factor | Concurrency levels | Engines | Warmup | Measured iterations | Status |
|--------------|-------------------|---------|--------|---------------------|--------|
| SF0.01 | 1 | duckcluster, duckdb-single | 1 | 3 | **Done** (smoke) |
| SF1 | 1, 4 | duckcluster, duckdb-single | 0–5 | 1–15 | **Done** (bulk-load + hardening) |
| SF10 | 1 | duckcluster, duckdb-single | 0–2 | 1–5 | **Done** (22/22 OK post shard-pin) |
| SF20 | 1 | duckcluster, duckdb-single | 1 | 2 | **Done** (22/22 OK; Q01/Q04 win) |
| SF40 | 1 | duckcluster, duckdb-single | 2 | 3 | **Done** (22/22 OK; 7/22 DC wins) |
| SF100 | 1, 4, 8 | duckcluster, duckdb-single | 5 | 15 | Pending (node stability) |

---

## Results — SF0.01 smoke (`sf0.01-20260710-204934`)

**Run parameters:** concurrency=1, warmup=1, measured=3, engine=both, 3 workers, 6 shards.

At SF0.01 the dataset is tiny (~60k `lineitem` rows total). Single-node DuckDB finishes most
queries in single-digit milliseconds. DuckCluster pays fixed distributed overhead (6 gRPC
fragments, broadcast `UNION ALL`, coordinator merge, prefetch) that dominates on this scale.

### Latency summary (p50 ms, concurrency=1)

| Query | DuckCluster p50 | Single-node p50 | Ratio (DC / single) | Fragments | Merge strategy |
|-------|-----------------|-----------------|---------------------|-----------|----------------|
| Q01 | 182 | 9 | 20.2× | 6 | GROUP_BY_MERGE |
| Q02 | 72 | 13 | 5.5× | 1 | TOP_K |
| Q03 | 284 | 9 | 31.6× | 6 | GROUP_BY_MERGE |
| Q04 | 279 | 9 | 31.0× | 6 | GROUP_BY_MERGE |
| Q05 | 220 | 19 | 11.6× | 6 | GROUP_BY_MERGE |
| Q06 | 155 | 2 | 77.5× | 6 | PARTIAL_AGG |
| Q07 | 350 | 13 | 26.9× | 6 | GROUP_BY_MERGE |
| Q08 | 321 | 14 | 22.9× | 6 | GROUP_BY_MERGE |
| Q09 | 1064 | 17 | 62.6× | 6 | GROUP_BY_MERGE |
| Q10 | 898 | 18 | 49.9× | 6 | GROUP_BY_MERGE |
| Q11 | 356 | 5 | 71.2× | 1 | GROUP_BY_MERGE |
| Q12 | 166 | 6 | 27.7× | 6 | GROUP_BY_MERGE |
| Q13 | 8597 | 13 | **661×** | 6 | NESTED_GROUP_BY_MERGE |
| Q14 | 167 | 6 | 27.8× | 6 | PARTIAL_AGG |
| Q15 | 793 | 6 | 132.2× | 6 | WITH_CTE_MERGE |
| Q16 | 1107 | 15 | 73.8× | 1 | GROUP_BY_MERGE |
| Q17 | 220 | 4 | 55.0× | 6 | PARTIAL_AGG |
| Q18 | 227 | 10 | 22.7× | 6 | GROUP_BY_MERGE |
| Q19 | 160 | 6 | 26.7× | 6 | PARTIAL_AGG |
| Q20 | 67 | 12 | 5.6× | 1 | TOP_K |
| Q21 | 274 | 25 | 11.0× | 6 | GROUP_BY_MERGE |
| Q22 | 110 | 11 | 10.0× | 1 | GROUP_BY_MERGE |

**SF0.01 headline:** DuckCluster **0/22** queries faster than single-node at concurrency=1.
Average p50 ratio **~67×** slower. This is expected — the data fits entirely in L3 cache on one
process; coordination cost is the entire story.

### Notable overhead patterns

| Pattern | Example | Observation |
|---------|---------|-------------|
| Multi-fragment fan-out | Q01, Q06 | 6 fragments + merge; worker time 9–32 ms each, end-to-end 155–182 ms |
| Coordinator two-step merge | Q13 | Worker time ≤54 ms, but p50 **8597 ms** — nested `GROUP BY` merge on coordinator dominates |
| CTE two-step merge | Q15 | p50 793 ms vs single-node 6 ms |
| Single-shard driving table | Q02, Q20, Q22 | Lowest ratios (5–10×) — only 1 fragment, less broadcast |

### Fragment skew (worker duration spread, ms)

| Query | worker-1 | worker-2 | worker-3 | Skew |
|-------|----------|----------|----------|------|
| Q01 | 25 | 32 | 12 | 20 |
| Q04 | — | 65 | 13 | 52 |
| Q06 | 20 | 24 | 9 | 15 |
| Q13 | — | 54 | 12 | 42 |

Skew is modest on workers; latency is not fragment-straggler bound at SF0.01 — merge and RPC
overhead dominate.

---

## Results — SF1 (`sf1-20260711-001805`, post merge-hardening)

**Run parameters:** concurrency=1, warmup=0, measured=1, engine=duckcluster, 3 workers, 6 shards.

Coordinator includes bulk-load (`DuckDbBulkInserter`), CTAS phase-1 handoff, fragment streaming
Appender, and `CoordinatorDuckDbPool` (see [DESIGN-DECISIONS.md §14](DESIGN-DECISIONS.md#14-challenges--solutions)).

Single-node baseline: `sf1-20260710-225016` (exec p50, same data).

### Optimization trajectory (exec p50 sum, c=1)

| Milestone | Run ID | Suite total | vs single-node | Notes |
|-----------|--------|-------------|----------------|-------|
| Pre-fix | — | minutes+ on Q13/Q16 | — | Row-by-row INSERT stall |
| Bulk-load | `sf1-20260710-232214` | 19,242 ms | **9.0×** | Appender; Q13 1226 ms |
| **Merge-hardening** | `sf1-20260711-001805` | **15,512 ms** | **7.2×** | **19% faster** than bulk-load |

### Key query movement (bulk-load → hardening)

| Query | Bulk-load p50 | Hardening p50 | Change | Merge strategy |
|-------|---------------|---------------|--------|----------------|
| Q15 | 210 ms | **136 ms** | −35% | WITH_CTE_MERGE |
| Q16 | 699 ms | **385 ms** | −45% | GROUP_BY_MERGE |
| Q07 | 4534 ms | **2926 ms** | −35% | GROUP_BY_MERGE |
| Q13 | 1226 ms | **1076 ms** | −12% | NESTED_GROUP_BY_MERGE |
| Q21 | 1237 ms | **752 ms** | −39% | GROUP_BY_MERGE |

### Full latency summary (p50 ms, concurrency=1)

| Query | DuckCluster p50 | Single-node p50 | Ratio | Fragments | Merge strategy |
|-------|-----------------|-----------------|:-----:|-----------|----------------|
| Q01 | 634 | 85 | 7.5× | 6 | GROUP_BY_MERGE |
| Q02 | 150 | 87 | 1.7× | 1 | TOP_K |
| Q03 | 2306 | 47 | 49.1× | 6 | GROUP_BY_MERGE |
| Q04 | 862 | 61 | 14.1× | 6 | GROUP_BY_MERGE |
| Q05 | 815 | 92 | 8.9× | 6 | GROUP_BY_MERGE |
| Q06 | 125 | 8 | 15.6× | 6 | PARTIAL_AGG |
| Q07 | 2926 | 105 | 27.9× | 6 | GROUP_BY_MERGE |
| Q08 | 372 | 89 | 4.2× | 6 | GROUP_BY_MERGE |
| Q09 | 817 | 215 | 3.8× | 6 | GROUP_BY_MERGE |
| Q10 | 923 | 172 | 5.4× | 6 | GROUP_BY_MERGE |
| Q11 | 34 | 25 | 1.4× | 1 | GROUP_BY_MERGE |
| Q12 | 193 | 31 | 6.2× | 6 | GROUP_BY_MERGE |
| Q13 | 1076 | 186 | 5.8× | 6 | NESTED_GROUP_BY_MERGE |
| Q14 | 133 | 36 | 3.7× | 6 | PARTIAL_AGG |
| Q15 | 136 | 43 | 3.2× | 6 | WITH_CTE_MERGE |
| Q16 | 385 | 133 | 2.9× | 1 | GROUP_BY_MERGE |
| Q17 | 1412 | 77 | 18.3× | 6 | PARTIAL_AGG |
| Q18 | 1107 | 197 | 5.6× | 6 | GROUP_BY_MERGE |
| Q19 | 194 | 43 | 4.5× | 6 | PARTIAL_AGG |
| Q20 | **76** | 81 | **0.9×** | 1 | TOP_K |
| Q21 | 752 | 235 | 3.2× | 6 | GROUP_BY_MERGE |
| Q22 | **84** | 101 | **0.8×** | 1 | GROUP_BY_MERGE |

**SF1 headline:** DuckCluster **2/22** queries faster than single-node (Q20, Q22). Suite **15.5 s**
vs single-node **2.1 s** (~**7.2×**). Hardening shaved ~3.7 s off bulk-load. Worst gaps: Q03 (49×),
Q07 (28×), Q17 (18×).

### SF1 concurrency=4 (`sf1-20260710-234611`, bulk-load only)

| Metric | c=1 bulk | c=4 bulk | Change |
|--------|----------|----------|--------|
| Suite exec p50 sum | 19,242 ms | 48,680 ms | **2.5× worse** |

Concurrency **hurts** latency today — coordinator merge serializes; 4 clients contend on one
coordinator DuckDB pool. Throughput crossover requires merge parallelism or worker-side final
aggregation, not raw client count alone.

---

## Results — SF10 smoke (`sf10-20260711-000816`)

**Run parameters:** concurrency=1, warmup=0, measured=1, engine=both, 3 workers, 9 shards (~14 GB).

### Headline

| Metric | Value |
|--------|-------|
| DuckCluster OK | **20/22** (Q10, Q18 ERROR — worker fragment failure on `lineitem_shard0/1`) |
| Suite exec p50 | DC **130,852 ms** vs single **27,856 ms** → **4.7×** |
| DuckCluster faster | **1/22** (Q02: 177 ms vs 322 ms) |
| Near parity | Q01 (1.3×), Q06 (1.2×) |

### Latency summary (p50 ms, concurrency=1)

| Query | DuckCluster p50 | Single-node p50 | Ratio | Status |
|-------|-----------------|-----------------|:-----:|--------|
| Q01 | 1427 | 1139 | 1.3× | OK |
| Q02 | **177** | 322 | **0.5×** | OK — DC wins |
| Q03 | 22739 | 1759 | 12.9× | OK |
| Q04 | 3364 | 1065 | 3.2× | OK |
| Q05 | 4738 | 1027 | 4.6× | OK |
| Q06 | 502 | 409 | 1.2× | OK |
| Q07 | 15441 | 1268 | 12.2× | OK |
| Q08 | 6399 | 1131 | 5.7× | OK |
| Q09 | 16695 | 4071 | 4.1× | OK |
| Q10 | — | 1658 | — | **ERROR** (worker) |
| Q11 | 510 | 177 | 2.9× | OK |
| Q12 | 4701 | 949 | 5.0× | OK |
| Q13 | 10579 | 3135 | 3.4× | OK |
| Q14 | 748 | 510 | 1.5× | OK |
| Q15 | 904 | 426 | 2.1× | OK |
| Q16 | 2951 | 506 | 5.8× | OK |
| Q17 | 24130 | 1237 | 19.5× | OK |
| Q18 | — | 2495 | — | **ERROR** (worker) |
| Q19 | 1569 | 922 | 1.7× | OK |
| Q20 | 5000 | 558 | 9.0× | OK |
| Q21 | 6334 | 2700 | 2.3× | OK |
| Q22 | 1944 | 392 | 5.0× | OK |

**SF10 vs SF1 trend:** Suite ratio improves **9× → 4.7×** as per-shard scan work grows. Q01/Q06
approach parity. Q03/Q07/Q17 remain outliers (coordinator merge + broadcast cost).

---

## Results — SF10 post cache-fix (`024011` + `031257`)

**Canonical SF10 comparison** after shard-pin fix. DuckCluster and single-node were run separately
with identical harness settings (warmup=2, measured=5, concurrency=1):

| Run ID | Engine | File |
|--------|--------|------|
| `sf10-20260711-024011` | duckcluster | 22/22 OK |
| `sf10-20260711-031257` | duckdb-single | 22/22 OK |

### Headline

| Metric | Pre-fix (`000816`) | Post-fix (combined) | Change |
|--------|-------------------|----------------------|--------|
| DuckCluster OK | 20/22 | **22/22** | Q10/Q18 fixed |
| Suite exec p50 | DC 130,852 ms / SN 27,856 ms | DC **161,910 ms** / SN **103,335 ms** | |
| Ratio vs single-node | **4.7×** | **1.6×** | **−66% gap** |
| DuckCluster faster | 1/22 (Q02) | **11/22** | Q01, Q02, Q04, Q06, Q11, Q14, Q15, Q19, Q20, Q21, Q22 |

Note: single-node baseline is **3.7× slower** in this re-run (103 s vs 28 s in the smoke run) —
likely cgroup contention from back-to-back benchmark activity. Treat ratios as directional; re-run
both engines in one `ENGINE=both` pass for a stricter comparison.

### Latency summary (p50 ms, concurrency=1)

| Query | DuckCluster | Single-node | Ratio | vs `000816` DC |
|-------|-------------|-------------|:-----:|----------------|
| Q01 | **1438** | 2606 | **0.55×** | 1.3× → **wins** |
| Q02 | **539** | 842 | **0.64×** | wins |
| Q03 | 6755 | 2483 | 2.7× | 22.7 s → 6.8 s |
| Q04 | **2082** | 3132 | **0.66×** | wins |
| Q05 | 5723 | 4253 | 1.4× | 4.7 s → 5.7 s |
| Q06 | **614** | 813 | **0.76×** | wins |
| Q07 | 7272 | 3430 | 2.1× | 15.4 s → 7.3 s |
| Q08 | 11445 | 3214 | 3.6× | 6.4 s → 11.4 s |
| Q09 | 30422 | 15088 | 2.0× | 16.7 s → 30.4 s |
| Q10 | 11238 | 5311 | 2.1× | ERROR → OK |
| Q11 | **313** | 907 | **0.35×** | wins |
| Q12 | 2701 | 2079 | 1.3× | 4.7 s → 2.7 s |
| Q13 | 18762 | 8933 | 2.1× | 10.6 s → 18.8 s |
| Q14 | **807** | 2393 | **0.34×** | wins |
| Q15 | **1376** | 1652 | **0.83×** | wins |
| Q16 | 5428 | 4058 | 1.3× | 3.0 s → 5.4 s |
| Q17 | 9684 | 4479 | 2.2× | 24.1 s → 9.7 s |
| Q18 | 30299 | 12476 | 2.4× | ERROR → OK |
| Q19 | **1991** | 3849 | **0.52×** | wins |
| Q20 | **1149** | 3229 | **0.36×** | wins |
| Q21 | **11130** | 14787 | **0.75×** | wins |
| Q22 | **742** | 3321 | **0.22×** | wins |

**Takeaway:** Merge pushdown + shard-pin brought the suite from **4.7× → 1.6×** and flipped
**11 queries faster than single-node**. Remaining outliers: Q08/Q09/Q13/Q18 (heavy merge + large
output), Q03/Q07 (broadcast + grouped merge).

---

## Results — SF20 (`sf20-20260711-042835`)

**Run parameters:** concurrency=1, warmup=1, measured=2, engine=both, 3 workers, 9 shards (~28 GB).

### Headline

| Metric | SF10 (post-fix) | SF20 | Trend |
|--------|-----------------|------|-------|
| DuckCluster OK | 22/22 | **22/22** | Stable |
| Suite exec p50 | DC 162 s / SN 103 s → **1.6×** | DC **252 s** / SN **66 s** → **3.8×** | Ratio widened† |
| DC faster than SN | 11/22 | **2/22** (Q01, Q04) | Scan winners only |
| DC scale SF10→SF20 | — | **1.55×** suite time for **2×** data | Sublinear |

†SF10 single-node baseline was run separately and may have been cgroup-contended (103 s suite).
This SF20 run is a fair `ENGINE=both` pass — SN at 66 s is a cleaner baseline. DC/SN ratio at SF20
is **3.8×**, not necessarily worse cluster performance vs a fair SN baseline.

### Latency summary (p50 ms)

| Query | DuckCluster | Single-node | Ratio | DC scale vs SF10 |
|-------|-------------|-------------|:-----:|------------------|
| Q01 | **2136** | 3091 | **0.69×** | 1.5× |
| Q02 | 679 | 595 | 1.1× | 1.3× |
| Q03 | 12154 | 2842 | 4.3× | 1.8× |
| Q04 | **1744** | 2342 | **0.74×** | 0.8× |
| Q05 | 9581 | 2966 | 3.2× | 1.7× |
| Q06 | 587 | 516 | 1.1× | 1.0× |
| Q07 | 12226 | 2711 | 4.5× | 1.7× |
| Q08 | 18317 | 2825 | 6.5× | 1.6× |
| Q09 | 45163 | 7997 | 5.6× | 1.5× |
| Q10 | 15116 | 3368 | 4.5× | 1.3× |
| Q13 | 27148 | 7090 | 3.8× | 1.4× |
| Q18 | 49587 | 6822 | 7.3× | 1.6× |

### Takeaways

- **22/22 OK** — shard-pin holds at 2× data; no cache eviction failures.
- **DuckCluster scan/GROUP BY scales ~1.5×** for 2× data (suite 162 s → 252 s) — better than linear.
- **Outliers unchanged:** Q08/Q09/Q13/Q18 (6–7× SN) grow ~1.5–1.6× from SF10, not explosively.
- **Q01/Q04 beat single-node** at SF20; confirms parallelism advantage at scale.
- **SF40 validated** — see next section for continued gains.

---

## Results — SF40 (`sf40-20260711-051834`)

**Run parameters:** concurrency=1, warmup=2, measured=3, engine=both, 3 workers, 12 shards (~56 GB).

### Headline

| Metric | SF20 | SF40 | Trend |
|--------|------|------|-------|
| DuckCluster OK | 22/22 | **22/22** | Stable |
| Suite exec p50 | DC 252 s / SN 66 s → 3.8× | DC **396 s** / SN **156 s** → **2.5×** | Gap closing |
| DC faster than SN | 2/22 (Q01, Q04) | **7/22** | Q02, Q06, Q11, Q20, Q22 join |
| DC scale SF20→SF40 | — | **1.57×** suite time for **2×** data | Sub-linear |
| SN scale SF20→SF40 | — | **2.36×** suite time for **2×** data | Super-linear |

### Latency summary (p50 ms, concurrency=1)

| Query | DuckCluster p50 | Single-node p50 | Ratio | Fragments | Merge strategy |
|-------|-----------------|-----------------|:-----:|-----------|----------------|
| Q01 | **3184** | 8041 | **0.40×** | 12 | GROUP_BY_MERGE |
| Q02 | **851** | 912 | **0.93×** | 1 | TOP_K |
| Q03 | 21847 | 6972 | 3.1× | 12 | GROUP_BY_MERGE |
| Q04 | **2178** | 5386 | **0.40×** | 12 | GROUP_BY_MERGE |
| Q05 | 15813 | 7114 | 2.2× | 12 | GROUP_BY_MERGE |
| Q06 | **741** | 1213 | **0.61×** | 12 | PARTIAL_AGG |
| Q07 | 20158 | 5963 | 3.4× | 12 | GROUP_BY_MERGE |
| Q08 | 29308 | 7201 | 4.1× | 12 | GROUP_BY_MERGE |
| Q09 | 67742 | 17586 | 3.9× | 12 | GROUP_BY_MERGE |
| Q10 | 21149 | 8086 | 2.6× | 12 | GROUP_BY_MERGE |
| Q11 | **519** | 762 | **0.68×** | 1 | GROUP_BY_MERGE |
| Q12 | 8394 | 3729 | 2.3× | 12 | GROUP_BY_MERGE |
| Q13 | 39362 | 16678 | 2.4× | 12 | NESTED_GROUP_BY_MERGE |
| Q14 | 3374 | 2451 | 1.4× | 12 | PARTIAL_AGG |
| Q15 | 5108 | 2003 | 2.6× | 12 | WITH_CTE_MERGE |
| Q16 | 11278 | 2993 | 3.8× | 1 | GROUP_BY_MERGE |
| Q17 | 28056 | 8087 | 3.5× | 12 | PARTIAL_AGG |
| Q18 | 81819 | 16379 | 5.0× | 12 | GROUP_BY_MERGE |
| Q19 | 6745 | 6309 | 1.07× | 12 | PARTIAL_AGG |
| Q20 | **4102** | 4731 | **0.87×** | 1 | TOP_K |
| Q21 | 21537 | 19488 | 1.11× | 12 | GROUP_BY_MERGE |
| Q22 | **2451** | 3877 | **0.63×** | 1 | GROUP_BY_MERGE |

### Takeaways

- **7/22 DC wins** — Q01, Q02, Q04, Q06, Q11, Q20, Q22 beat single-node on the same hardware.
- **Near-parity:** Q19 (1.07×), Q21 (1.11×) — approaching crossover at next scale step.
- **Suite ratio 3.8× → 2.5×** from SF20 — gap continues closing as scan work dominates.
- **DC scales sub-linearly** (1.57× for 2× data) while **SN scales super-linearly** (2.36× for 2× data) — the fundamental crossover dynamic.
- **Strong wins on scan-heavy:** Q01/Q04 at 0.40× (DC 2.5× faster); Q06 at 0.61×.
- **Persistent outliers:** Q08/Q09/Q18 remain 4–5× slower — broadcast `UNION ALL` per fragment is the root cause.
- **12 shards** (vs 9 at SF20) — 4 per worker; additional parallelism helps scan queries without increasing merge cost.

### DC wins breakdown

| Query | Why DC wins | Type |
|-------|-------------|------|
| Q01 | Full lineitem scan split across 3 workers; trivial GROUP BY merge | Scan + agg |
| Q04 | orders scan parallelized; EXISTS subquery local per shard | Scan + filter |
| Q06 | Embarrassingly parallel SUM; PARTIAL_AGG merge near-zero cost | Pure scan |
| Q11 | Single-fragment; worker handles entire `partsupp` dimension locally | Local compute |
| Q22 | Single-fragment GROUP BY; SN hash table overhead dominates at scale | Local compute |
| Q02 | TOP_K on single worker partition; SN scans full table | Partition prune |
| Q20 | TOP_K prune; worker returns pre-sorted subset | Partition prune |

---

## Crossover analysis

| Question | SF0.01 | SF1 | SF10 | SF20 | SF40 | Trend |
|----------|--------|-----|------|------|------|-------|
| Suite ratio (DC/SN) | 66× | 7.2× | 4.7× | 3.8× | **2.5×** | Converging → 1.0× |
| DC wins (out of 22) | 0 | 2 | 1 | 2 | **7** | Accelerating |
| DC sub-linear scaling | — | — | — | 1.55× / 2× data | 1.57× / 2× data | Consistent |
| SN super-linear scaling | — | — | — | 2.67× / 2× data (Q01) | 2.36× avg / 2× data | Memory/cache pressure |

**Per-query crossover progression:**

| Query | SF0.01 | SF1 | SF10 | SF20 | SF40 | Category |
|-------|--------|-----|------|------|------|----------|
| Q01 | 20.2× | 7.5× | 1.3× | **0.69×** | **0.40×** | Scan — DC dominates at scale |
| Q04 | 31.0× | 14.1× | 3.2× | **0.74×** | **0.40×** | Scan — DC dominates at scale |
| Q06 | 77.5× | 15.6× | 1.2× | 1.1× | **0.61×** | Scan — crossed over at SF40 |
| Q11 | 71.2× | 1.4× | 2.9× | 1.3× | **0.68×** | Local — crossed over at SF40 |
| Q22 | 10.0× | 0.8× | 5.0× | 1.03× | **0.63×** | Local — crossed over at SF40 |
| Q02 | 5.5× | 1.7× | 0.5× | 1.1× | **0.93×** | TOP_K — DC wins at SF40 |
| Q20 | 5.6× | 0.9× | 9.0× | 1.2× | **0.87×** | TOP_K — DC wins at SF40 |
| Q19 | 26.7× | 4.5× | 1.7× | 1.3× | 1.07× | Near-parity — next to cross |
| Q21 | 11.0× | 3.2× | 2.3× | 1.9× | 1.11× | Near-parity — next to cross |

**Scan-heavy queries cross over first** because per-worker scan cost scales as `data / num_workers`
while single-node scans scale as `data` — at large enough SF, the 3× parallelism advantage
overtakes the fixed coordination overhead.

**Join/merge-heavy queries (Q03, Q07, Q08, Q09, Q18) remain 3–5× slower** — their bottleneck is
coordinator merge cost and broadcast `UNION ALL` per fragment, not scan time. These need broadcast
join reform (open engineering work) to cross over.

**Throughput crossover** (DuckCluster faster under concurrent clients) is **not yet demonstrated**
— c=4 degraded SF1 latency. Requires parallel merge execution or more workers, not just more clients.

---

## Remaining bottlenecks

Ordered by measured impact. Resolved items struck through.

| # | Bottleneck | Symptom | Status |
|---|------------|---------|--------|
| 1 | ~~Row-by-row temp-table INSERT~~ | Q13/Q16 stuck minutes | **Resolved** — `DuckDbBulkInserter` |
| 2 | ~~Phase-1 Java ping-pong~~ | Q13/Q15 extra heap copy | **Resolved** — CTAS handoff |
| 3 | **Broadcast `UNION ALL` in every fragment** | Q03/Q07/Q09 worst ratios; each fragment scans all broadcast shard copies | **Open** — planner emits full broadcast per fragment |
| 4 | **Coordinator merge SQL cost** | Q03 exec 2.3 s (SF1), 22.7 s (SF10) with workers ≤845 ms; Q07 similar | **Partial** — Opt 5 pushdown reduces fan-in; broadcast still open |
| 5 | **Final result materialization** | Q13 client 1973 ms vs exec 1076 ms; full `ResultSet` → Java → JSON | **Open** — no streaming response |
| 6 | **gRPC + fragment RPC overhead** | Fixed ~50–200 ms per query regardless of data size | **Open** — 6–9 fragments × round-trip |
| 7 | **Worker broadcast cache eviction** | Q07/Q10/Q17/Q18: `orders_shard0` evicted mid-fragment | **Fixed** — shard pin session + larger cache |
| 8 | **Coordinator merge serialization** | c=4 suite 2.5× slower than c=1 | **Partial** — pool added; no parallel merge |
| 9 | **All-string column proto** | Parse overhead at bulk insert | **Open** — typed cells end-to-end |
| 10 | **6–9 fragments on 3 workers** | 2–3 waves of fragment execution | **Open** — more workers/shards help |

---

## How to overtake single-node DuckDB

### What already works

- **Scale amortizes coordination** — suite ratio 7.2× (SF1) → 3.8× (SF20) → **2.5× (SF40)**.
- **7/22 queries beat single-node at SF40** on identical hardware (same machine, DC running 4 processes vs SN's 1).
- **DC 60% faster on scan-heavy queries** — Q01/Q04 at 0.40× ratio (SF40).
- **Sub-linear DC scaling** — 1.57× latency for 2× data consistently.
- **Merge path is no longer broken** — bulk-load + hardening removed minutes-long stalls.

### Path to latency parity (c=1)

| Priority | Action | Queries helped | Expected impact |
|----------|--------|----------------|-----------------|
| 1 | **Broadcast join reform** — shared dimension cache per worker, not `UNION ALL` per fragment | Q03, Q07, Q09, Q05 | High — removes redundant scans |
| 2 | **Merge pushdown** — hierarchical per-worker collapse + TOP-K oversample on fragments | Q03, Q07, Q16, Q17 | **Done (Opt 5)** — re-benchmark to measure |
| 3 | **Stream results** — chunked HTTP or Arrow; skip `toQueryResult` monolith | Q13, Q18, large outputs | Medium — fixes client gap |
| 4 | **Fix SF10 worker failures** — broadcast cache eviction | Q10, Q18 | **Done** — shard pin session |
| 5 | **Typed proto columns** — `DOUBLE`/`BIGINT` from workers, direct Appender | All merge queries | Medium at SF10+ |
| 6 | **More shards + workers** — 9→12+ shards, 4–6 workers | Scan-heavy Q01/Q06 | Per-shard work hides RPC |

### Path to throughput superiority (c≥4)

Single-node DuckDB serializes on one process. DuckCluster should win when **independent queries**
saturate workers in parallel — but only if the coordinator keeps up.

| Priority | Action | Why |
|----------|--------|-----|
| 1 | **Parallel merge executor** — dedicated pool, not HTTP thread | c=4 currently 2.5× worse |
| 2 | **Merge backpressure** — cap concurrent coordinator DuckDB merges | Prevents p95 blowout |
| 3 | **Worker-side final aggregation** — coordinator merges summaries only | Shrinks merge CPU + memory |
| 4 | **Multi-coordinator** (longer term) — partition merge by query | Removes single coordinator ceiling |

### Realistic crossover targets

| Scenario | Target | Current | Status |
|----------|--------|---------|--------|
| SF10, c=1, scan-heavy (Q01/Q06) | ≤1.5× or faster | **0.40–0.61×** | **Met** — DC 40–60% faster |
| SF40, c=1, per-query wins | Majority of scan queries | **7/22 faster** | **Partial** — scan/local wins; join losers remain |
| SF40, c=1, full suite | ≤2× single-node | **2.5×** | Close — broadcast reform would close gap |
| SF40, c=8, throughput | DC QPS > single-node | Not measured | Needs parallel merge |
| SF100+, c=1 | DC faster on majority | Not run | Node stability blocking higher SF |

---

## Success criteria

| Scenario | Target | Result |
|----------|--------|--------|
| SF1, 1 client | DuckCluster within 2× single-node | **Not met** — 7.2× (hardened); 2/22 faster |
| SF10, 1 client | 22/22 queries OK | **Met** — post shard-pin |
| SF40, 1 client | DC wins on scan-heavy queries | **Met** — 7/22 faster; Q01/Q04 at 0.40× |
| SF40, 1 client | Suite within 2× single-node | **Partial** — 2.5×; needs broadcast reform |
| SF40, 1 client | 22/22 queries OK | **Met** |
| SF100+, any concurrency | DuckCluster dominates on majority | **Blocked** — node stability issues at higher SF |
| Correctness | All 22 queries match baseline | **PASS** (integration gate) |

---

## Run log

| Run ID | Date | SF | Concurrency | Warmup | Iterations | Notes |
|--------|------|----|-------------|--------|------------|-------|
| sf0.01-20260709-124130 | 2026-07-09 | 0.01 | 1 | 0 | 1 | Pre-planner; 17 queries failed/unsupported |
| sf0.01-20260710-204934 | 2026-07-10 | 0.01 | 1 | 1 | 3 | **22/22 OK** both engines; first valid comparison |
| sf1-20260710-225016 | 2026-07-10 | 1 | 1 | 0 | 1 | Single-node baseline (all 22 queries 8–235 ms) |
| sf1-20260710-232214 | 2026-07-11 | 1 | 1 | 0 | 1 | Post bulk-load DuckCluster; ~9× vs single-node |
| sf1-20260710-234611 | 2026-07-11 | 1 | 4 | 0 | 1 | c=4 bulk-load; 2.5× worse than c=1 |
| sf1-20260711-001805 | 2026-07-11 | 1 | 1 | 0 | 1 | **Post hardening**; 7.2× vs single-node; 19% faster than bulk |
| sf1-20260711-004753 | 2026-07-11 | 1 | 1 | 5 | 15 | **Post pushdown**; 3.2× vs single-node; 22/22 OK |
| sf10-20260711-024011 | 2026-07-11 | 10 | 1 | 2 | 5 | DC 22/22 OK (shard-pin fix) |
| sf20-20260711-042835 | 2026-07-11 | 20 | 1 | 1 | 2 | **22/22 OK** both engines; 3.8× vs SN; Q01/Q04 win |
| sf40-20260711-051834 | 2026-07-11 | 40 | 1 | 2 | 3 | **22/22 OK** both engines; **2.5×** vs SN; **7/22 DC wins** |
| sf10-20260711-031257 | 2026-07-11 | 10 | 1 | 2 | 5 | SN baseline re-run (paired with 024011) |
| sf10-20260711-000816 | 2026-07-11 | 10 | 1 | 0 | 1 | Pre-pushdown smoke; 4.7×; Q10/Q18 ERROR |

---

## Remaining blockers (engineering)

| Blocker | Status | Notes |
|---------|--------|-------|
| Planner correctness (22 queries) | **Resolved** | Integration + fragment tests green |
| Merge row-by-row insert | **Resolved** | `DuckDbBulkInserter` |
| Phase-1 Java ping-pong | **Resolved** | CTAS handoff (see [DESIGN-DECISIONS.md §14](DESIGN-DECISIONS.md#14-challenges--solutions)) |
| SF10 worker fragment failures | **Resolved** | Shard-pin session + larger cache |
| Broadcast `UNION ALL` per fragment | **Open** | Top latency blocker for Q03/Q07/Q08/Q09/Q18 (3–5×) |
| Coordinator merge SQL at scale | **Open** | Q03 22 s at SF10 despite sub-second workers |
| Concurrency throughput | **Open** | c=4 hurts; needs parallel merge |
| Node stability at SF40+ | **Open** | Workers crash under sustained load; SSH hangs |

---

## Will DuckCluster beat single-node DuckDB?

### Short answer (updated 2026-07-11)

| Workload | Result | Evidence |
|----------|--------|----------|
| SF0.01, 1 client | **No** | 5–661× slower; data fits in L3 cache |
| SF1, 1 client | **No** | 7.2× suite; 2/22 faster (Q20/Q22) |
| SF10, 1 client, scan-heavy | **Yes** | Q01 0.55×, Q06 0.76× |
| SF20, 1 client | **Partial** | 3.8× suite; 2/22 faster (Q01/Q04) |
| SF40, 1 client, scan-heavy | **Yes** | Q01/Q04 at 0.40×, Q06 at 0.61× — DC **60% faster** |
| SF40, 1 client, full suite | **Partial** | 2.5× suite; **7/22 faster**; 2 more near-parity |
| SF40+, c≥8 throughput | **Not yet measured** | Needs parallel merge |

DuckCluster is **not** designed to win on micro-benchmarks. It is designed to win when **per-shard
work is large enough** to hide RPC/merge cost, and when **multiple clients** need throughput
single-node DuckDB cannot provide from one process.

### Plus points that help DuckCluster go faster

1. **Embarrassingly parallel fragments** — `lineitem`/`orders` split 6 ways; each worker scans
   ~1/6 of the partitioned data with no cross-worker coordination during the scan.

2. **Concurrent query throughput** — fragments dispatch on a thread pool; multiple clients can
   saturate 3 workers while single-node DuckDB queues on one connection pool.

3. **Horizontal scale-out** — adding workers and shards reduces per-fragment data volume linearly
   (not available to single-node).

4. **Data locality** — each worker reads only its local `.duckdb` shard files; at large SF this
   avoids one process juggling more data than fits in memory.

5. **Broadcast dimension tables are tiny** — `nation`, `region`, `supplier`, etc. replicate cheaply;
   the heavy tables are partitioned.

6. **TOP_K / partial-agg pushdown** — **done (Opt 5):** hierarchical per-worker collapse +
   TOP-K oversample on fragments (`LIMIT × shard_count`); coordinator merges smaller intermediates.
   Re-benchmark pending to quantify Q03/Q07 impact.

### Minuses that still hurt (measured at SF1/SF10)

1. **Broadcast `UNION ALL`** — every fragment re-scans all broadcast shard copies; worst on Q03/Q07.
2. **Coordinator merge SQL** — workers finish in &lt;1 s but Q03/Q07 add multi-second (SF1) or
   multi-tens-of-second (SF10) coordinator time.
3. **Result materialization + HTTP** — Q13 client latency ~2× execution time.
4. **gRPC fragment overhead** — fixed per-query tax; hurts most at small SF.
5. **Coordinator merge serialization** — c=4 amplifies latency; pool alone insufficient.
6. **Worker failures at SF10** — Q10/Q18 fragment errors block fair comparison.

See [DESIGN-DECISIONS.md §14](DESIGN-DECISIONS.md#14-challenges--solutions) for the full
before/after optimization history (bulk load, CTAS handoff, streaming Appender, connection pool,
merge pushdown).

---

## Harness reference

| Script / module | Purpose |
|-----------------|---------|
| `scripts/benchmark-datagen.sh` | Generate sharded TPC-H data + `baseline.duckdb` |
| `scripts/benchmark-redistribute.sh` | Rebalance workers from `staging/` (no regen) |
| `scripts/benchmark-run.sh` | Start cluster, run harness, write JSON |
| `benchmark/` | Java harness JAR (`BenchmarkMain`) |

**Commands:** [BENCHMARK-COMMANDS.md](BENCHMARK-COMMANDS.md) — copy-paste runbook (datagen separate from execution).

**Results JSON:** `benchmark/results/*.json`. Primary metric: **`execution_ms`**. Output schema per iteration:

**Visualizations:** `python3 scripts/benchmark-plots.py` writes:

| File | Use |
|------|-----|
| `docs/plots/benchmark-hero.png` | README primary chart |
| `docs/plots/crossover-heatmap.png` | Per-query ratio heatmap SF0.01–SF40 (teal/violet palette) |
| `docs/plots/scaling-curves.png` | Suite time SF10–SF40 |
| `docs/plots/sf40-diverging.png` | SF40 dumbbell (all 22 queries) |
| `docs/plots/benchmark-summary.png` | Four-panel composite |

Requires matplotlib only.

```json
{
  "queryId": "Q01", "engine": "duckcluster", "scaleFactor": 10,
  "concurrency": 1, "iteration": 3,
  "latencyMs": { "execution": 1427, "client": 1512 },
  "status": "OK",
  "fragments": [{ "workerId": "worker-1", "durationMs": 312 }, ...]
}
```

**Fairness:** Both engines run on the same hardware, same JVM, same data files. DuckCluster uses 4 processes (1 coordinator + 3 workers) vs single-node DuckDB's 1 process — all on the same machine.

**Next step:** Broadcast-join reform for Q03/Q07/Q08/Q09/Q18 (the remaining 3–5× outliers); SF100 when node stability is resolved.

*Historical planning docs (PLAN-\*, PROPOSAL-\*) have been consolidated into [DESIGN-DECISIONS.md](DESIGN-DECISIONS.md) and this document.*
