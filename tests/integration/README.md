# DuckCluster integration tests

Pytest harness for distributed correctness, resilience, replication, and topology scenarios.

## Run

```bash
# Default: bundled 10-row sample CSV
./scripts/run-integration-tests.sh

# Larger synthetic dataset (1000 rows, generated at cluster startup)
./scripts/run-integration-tests.sh --demo-rows 1000

# Custom CSV file
./scripts/run-integration-tests.sh --demo-csv /path/to/events.csv
```

## Sample data

The repo includes a small committed CSV at `tests/integration/data/demo-events.csv` (10 rows).
this into per-cluster working directories under `data/integration/` (gitignored) and shard it
automatically. You can also split it yourself before running tests:

```bash
mkdir -p data/worker-{1,2,3}
./scripts/split-and-distribute.sh \
  --source tests/integration/data/demo-events.csv \
  --table events \
  --key id \
  --shards 6 \
  --workers worker-1,worker-2,worker-3 \
  --dirs data/worker-1,data/worker-2,data/worker-3 \
  --rf 2
```

Expected results are computed on the fly by running the same SQL against the source CSV in local
DuckDB (`duckcluster/baseline.py`), so correctness checks work for any dataset size.

## Pytest options

| Option | Description |
|--------|-------------|
| `--demo-csv PATH` | Source events CSV (default: `tests/integration/data/demo-events.csv`) |
| `--demo-rows N` | Generate an N-row synthetic CSV instead of `--demo-csv` |

## Layout

```
tests/integration/
├── data/demo-events.csv       # Bundled 10-row sample (committed)
├── manifest.yaml              # Correctness scenario catalog
├── queries/                   # SQL under test
├── duckcluster/               # Cluster helpers, baseline, compare
└── test_*.py                  # Scenario tests (see per-test docstrings)
```
