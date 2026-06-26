# Benchmark commands

Copy-paste commands for TPC-H benchmarking. **Data generation is separate from execution runs** — benchmark timing measures query execution only (`execution_ms` in results JSON).

See [BENCHMARK.md](BENCHMARK.md) for results and analysis.

---

## 0. Disk cleanup before larger scale factors

Benchmark data and logs can consume tens of GB (SF10 ≈ 14 GB data + 19 GB logs). Before generating SF50+:

```bash
# Preview what would be removed (keeps sf10, drops sf0.01 + sf1, prunes old logs)
DRY_RUN=1 ./scripts/benchmark-cleanup.sh --keep sf10 --logs

# Actually free space
./scripts/benchmark-cleanup.sh --keep sf10 --logs

# Generate next SF (replace + clean other scale factors)
./scripts/benchmark-datagen.sh 50 12 benchmark/data/sf50 --replace --clean-other-sf
```

`--clean-other-sf` on datagen calls cleanup automatically, keeping only the target output dir.

---

```bash
cd /root/DuckCluster

# Add benchmark module to a fresh clone (already done if pom.xml lists benchmark/)
mvn -q package -DskipTests -pl coordinator,worker,benchmark -am
```

---

## 1. Generate data (not timed)

Default placement is **balanced** (round-robin across workers). For 3 workers at SF1, 6 shards is fine; at SF10+ prefer 9 or 12 shards.

```bash
# SF1 — balanced placement (recommended)
./scripts/benchmark-datagen.sh 1 6 benchmark/data/sf1

# SF1 — more shards for larger scale factors (3 workers → 9 shards = 3 primary per worker)
./scripts/benchmark-datagen.sh 1 9 benchmark/data/sf1

# Ring placement (legacy consistent-hash; can skew load onto one worker)
./scripts/benchmark-datagen.sh 1 6 benchmark/data/sf1
# pass --placement ring inside datagen:
# scripts/ensure-python-duckdb.sh scripts/benchmark_datagen.py 1 6 benchmark/data/sf1 --placement ring
```

---

## 2. Rebalance existing SF1 data (no TPC-H regen)

If `benchmark/data/sf1/staging/` still exists from generation, redistribute to workers in ~seconds:

```bash
./scripts/benchmark-redistribute.sh benchmark/data/sf1 balanced
```

Your current SF1 was generated with **ring** placement (worker-2 holds more `orders` shards). Redistributing with **balanced** evens replica counts: 4 `lineitem` + 4 `orders` files per worker instead of 13/13/10.

Verify balance:

```bash
du -sh benchmark/data/sf1/workers/*
ls benchmark/data/sf1/workers/worker-*/lineitem*.duckdb | wc -l   # per worker
```

---

## 3. Execution-only benchmark runs

**Always** use `SKIP_DATAGEN=1`. Build once, then reuse JARs with `SKIP_BUILD=1`.

After coordinator merge changes (e.g. bulk temp-table load in `docs/PLAN-merge-bulk-load.md`), rebuild
before benchmarking — omit `SKIP_BUILD=1` on the first run, or run `mvn -pl coordinator -am package -DskipTests`.
With `VERBOSE=1`, the harness prints each query as it completes during the measured phase (`[n/total]`).

```bash
# SF1 smoke — both engines, verbose per-query output
SKIP_DATAGEN=1 VERBOSE=1 CONCURRENCY=1 WARMUP=1 ITERATIONS=3 ENGINE=both \
  ./scripts/benchmark-run.sh 1

# SF1 — single client, production-like iterations (positional or flags)
SKIP_DATAGEN=1 SKIP_BUILD=1 VERBOSE=1 CONCURRENCY=1 WARMUP=5 ITERATIONS=15 ENGINE=both \
  ./scripts/benchmark-run.sh 1

# Same run using flags
./scripts/benchmark-run.sh --sf 1 --concurrency 1 --skip-datagen --skip-build --verbose \
  --warmup 5 --iterations 15 --engine both

# SF1 — concurrency sweep (DuckCluster throughput)
SKIP_DATAGEN=1 SKIP_BUILD=1 VERBOSE=1 CONCURRENCY=1,4,8 WARMUP=5 ITERATIONS=15 ENGINE=both \
  ./scripts/benchmark-run.sh 1

# Single-node baseline only
SKIP_DATAGEN=1 SKIP_BUILD=1 VERBOSE=1 CONCURRENCY=1 WARMUP=5 ITERATIONS=15 ENGINE=duckdb-single \
  ./scripts/benchmark-run.sh 1

# DuckCluster only
SKIP_DATAGEN=1 SKIP_BUILD=1 VERBOSE=1 CONCURRENCY=1 WARMUP=5 ITERATIONS=15 ENGINE=duckcluster \
  ./scripts/benchmark-run.sh 1
```

Results land in `benchmark/results/sf1-<timestamp>.json`.

---

## 4. Read results

Primary field: **`execution_ms`** (server-side query time).

- **DuckCluster:** coordinator `stats.durationMs` (fragments + merge; excludes HTTP overhead)
- **Single-node:** DuckDB query time
- **`client_latency_ms`:** full HTTP round-trip (DuckCluster only); use for client-facing SLA, not engine comparison

Quick summary:

```bash
python3 - <<'PY'
import json, sys, glob
path = sorted(glob.glob("benchmark/results/sf1-*.json"))[-1]
data = json.load(open(path))
print("run:", data["run_id"])
for r in sorted(data["results"], key=lambda x: (x["engine"], x["query"])):
    if r.get("status") not in ("OK", "PARTIAL"):
        continue
    ex = r.get("execution_ms", r.get("latency_ms", {}))
    print(f"{r['engine']:14} {r['query']} c={r['concurrency']} exec_p50={ex.get('p50')}ms status={r['status']}")
PY
```

---

## 5. Correctness check (separate from benchmark timing)

```bash
cd tests/integration
.venv/bin/python -m pytest test_tpch.py::test_tpch_correctness -m tpch -q
```

---

## 6. SF10+ (when ready)

```bash
# Generate (takes longer — not part of benchmark timing)
./scripts/benchmark-datagen.sh 10 12 benchmark/data/sf10

# Run matrix
SKIP_DATAGEN=1 SKIP_BUILD=1 VERBOSE=1 CONCURRENCY=1,4,8,16 WARMUP=5 ITERATIONS=15 ENGINE=both \
  ./scripts/benchmark-run.sh 10
```

---

## Distribution tips

| Goal | Command / setting |
|------|-------------------|
| Even shard files per worker | `--placement balanced` or `benchmark-redistribute.sh` |
| More parallelism per query | Increase shard count (`9` or `12` for 3 workers) |
| Less duplicate I/O on workers | `--rf 1` (no replicas; trade-off: less fault tolerance) |
| Match production ring routing | `--placement ring` |

**Recommended for SF1 with 3 workers:** redistribute with `balanced`, then benchmark with `SKIP_DATAGEN=1 VERBOSE=1`.
