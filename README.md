# DuckCluster

A **distributed SQL query coordinator** for analytical workloads. Clients submit SQL over a REST API; DuckCluster plans the query with Apache Calcite, executes shard-local fragments on **DuckDB worker nodes**, streams partial results over gRPC, and merges them into a final answer.

Built in Java 17 as a multi-module Maven project.

For design details, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and [`docs/DESIGN-DECISIONS.md`](docs/DESIGN-DECISIONS.md).

---

## How it works

```
Client SQL
    │
    ▼
Coordinator (REST + Calcite planner)
    ├─ Classify tables (driving vs broadcast), detect merge strategy
    ├─ Generate N fragment SQL (qualified catalog.table names, UNION ALL for broadcast)
    ├─ Resolve target workers (3-tier: ring owners → cached → remote read)
    ├─ Prefetch broadcast shards to target workers (if JOIN)
    └─ Dispatch fragments in parallel via persistent gRPC channels
           │
           ▼
    Worker 0 … Worker N  (embedded DuckDB per node, all data local)
           │
           ▼
Coordinator merges partial results (embedded DuckDB)
           │
           ▼
    JSON response
```

**Sharding model:** data is pre-split into `.duckdb` shard files (e.g. `events_shard0.duckdb`) via `split-and-distribute.sh` and distributed to worker data directories independently per table. Workers auto-discover and ATTACH these files at startup. A consistent hash ring determines shard placement with configurable replication factor. Tables can have different shard counts and keys — the coordinator handles JOINs via broadcast shuffle at query time.

**Merge strategies:**

| Strategy | Use case | Worker | Coordinator |
|----------|----------|--------|-------------|
| `CONCATENATE` | `SELECT *`, filters | Return matching rows | Append batches |
| `PARTIAL_AGG` | `SELECT COUNT(*), SUM(x)` | Partial aggregates | Re-aggregate in DuckDB |
| `GROUP_BY_MERGE` | `SELECT k, COUNT(*) … GROUP BY k` | Partial grouped rows | Final `GROUP BY` in DuckDB |
| `TOP_K` | `ORDER BY … LIMIT n` | Local top-K per shard | Global sort + limit |

---

## Quick start

### Prerequisites

- Java 17+
- Maven 3.8+
- Python 3.10+ (integration tests)
- `curl` (and optionally `python3` for pretty JSON)
- `duckdb` CLI recommended for `start-cluster.sh` (Python `duckdb` package is enough for integration tests)

### Build

```bash
mvn clean package
```

### Run a local cluster

```bash
./scripts/start-cluster.sh
```

This builds the project, splits the bundled sample CSV into shard files, and starts one coordinator (HTTP `8080`, gRPC `9090`) and three workers (`9101`–`9103`).

The default source file is `tests/integration/data/demo-events.csv` (10 rows). Override with:

```bash
DUCKCLUSTER_DEMO_CSV=/path/to/events.csv ./scripts/start-cluster.sh
```

### Submit a query

```bash
# Simple scan with filter pushdown
curl -s -X POST http://127.0.0.1:8080/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT * FROM events WHERE id > 2"}' | python3 -m json.tool

# Distributed GROUP BY
curl -s -X POST http://127.0.0.1:8080/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT category, COUNT(*) AS cnt FROM events GROUP BY category"}' | python3 -m json.tool
```

### Cluster health

```bash
curl -s http://127.0.0.1:8080/v1/cluster/health
curl -s http://127.0.0.1:8080/v1/cluster/workers
```

### Integration tests

Integration tests live under `tests/integration/`. They start ephemeral clusters, shard the source CSV automatically, and compare distributed results against a single-node DuckDB baseline.

```bash
# First-time setup: creates a venv and installs pytest + duckdb
./scripts/run-integration-tests.sh
```

**Options** (passed through to pytest):

```bash
# Default — bundled 10-row sample CSV (tests/integration/data/demo-events.csv)
./scripts/run-integration-tests.sh

# Larger synthetic dataset
./scripts/run-integration-tests.sh --demo-rows 1000

# Custom CSV
./scripts/run-integration-tests.sh --demo-csv /path/to/events.csv
```

**Manual shard setup** (optional — tests shard data themselves, but you can pre-split for exploration):

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

Unit tests only:

```bash
mvn test
```

See [`tests/integration/README.md`](tests/integration/README.md) for harness layout and scenario details.

---

## REST API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/query` | Execute SQL synchronously |
| `GET` | `/v1/cluster/health` | Cluster liveness summary |
| `GET` | `/v1/cluster/workers` | Registered workers and heartbeat status |

**Query request**

```json
{ "sql": "SELECT category, COUNT(*) AS cnt FROM events GROUP BY category" }
```

**Query response**

```json
{
  "queryId": "…",
  "columns": ["category", "cnt"],
  "rows": [["A", 3], ["B", 3], ["C", 3]],
  "stats": {
    "mergeStrategy": "GROUP_BY_MERGE",
    "workersUsed": 3,
    "fragmentsExecuted": 3,
    "durationMs": 120,
    "workerDurationsMs": { "worker-1": 40, "worker-2": 35, "worker-3": 45 }
  }
}
```

---

## Configuration

Environment variables (defaults shown):

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKCLUSTER_COORDINATOR_HOST` | `127.0.0.1` | Coordinator bind/connect host |
| `DUCKCLUSTER_COORDINATOR_HTTP_PORT` | `8080` | REST API port |
| `DUCKCLUSTER_COORDINATOR_GRPC_PORT` | `9090` | Worker registration / heartbeat port |
| `DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC` | `5` | Heartbeat period (seconds) |
| `DUCKCLUSTER_HEARTBEAT_MISS_THRESHOLD` | `3` | Missed beats before worker removed |
| `DUCKCLUSTER_SHARD_COUNT` | `3` | Logical shard count for demo catalog |
| `DUCKCLUSTER_DATA_DIR` | `./data` | Worker data directory for shard files |
| `DUCKCLUSTER_POOL_SIZE` | `max(2, min(4, CPUs-1))` | DuckDB connections per worker |
| `DUCKCLUSTER_POOL_WAIT_MS` | `200` | Max wait for a free connection (ms) |
| `DUCKCLUSTER_REPLICATION_FACTOR` | `2` | Shard copies across workers |
| `DUCKCLUSTER_VNODES_PER_WORKER` | `100` | Virtual nodes in hash ring |
| `DUCKCLUSTER_WATCHER_INTERVAL_MS` | `2000` | Shard file polling interval (ms) |

---

## Project layout

```
DuckCluster/
├── proto/           # Protobuf + gRPC service definitions
├── common/          # Models, config, Calcite planner, consistent hash ring
├── coordinator/     # REST API, query execution, shard catalog, replication
├── worker/          # gRPC server, DuckDB pool, shard manager, file watcher
├── scripts/         # start-cluster.sh, split-and-distribute.sh, test helpers
├── tests/integration/  # Pytest harness + bundled sample CSV
└── docs/
    ├── ARCHITECTURE.md
    └── DESIGN-DECISIONS.md
```

**Run manually**

```bash
# Coordinator
java -jar coordinator/target/duckcluster-coordinator-0.1.0-SNAPSHOT.jar

# Worker (repeat with worker-1, worker-2, …)
java -jar worker/target/duckcluster-worker-0.1.0-SNAPSHOT.jar worker-1 127.0.0.1 9101
```

---

## Technology stack

| Layer | Choice |
|-------|--------|
| Language / build | Java 17, Maven |
| REST | Javalin 6.x |
| SQL planning | Apache Calcite 1.41 (`DuckDBSqlDialect`) |
| Worker OLAP | DuckDB JDBC |
| RPC | gRPC + Protobuf |
| Merge engine | Embedded DuckDB on coordinator |
| Tests | JUnit 5 (unit), pytest (integration) |

---

## Upcoming

| Phase | Focus |
|-------|--------|
| **G** | Benchmarking harness — TPC-H at multiple scale factors, compare against single-node DuckDB |
| **H** | HTML dashboard, SSE event stream, broader integration tests |
| **I** | Apache Arrow transfer, Docker Compose, load-aware scheduler |

---

## Demo data

`tests/integration/data/demo-events.csv` is a committed 10-row sample (`id`, `name`, `score`, `category`).
`start-cluster.sh` and the integration harness use it by default. `split-and-distribute.sh` turns it into
per-worker `.duckdb` shard files; workers discover and ATTACH them via `ShardFileWatcher`.

---

## Development

```bash
# Unit tests
mvn test

# Integration tests (see Integration tests section above)
./scripts/run-integration-tests.sh

# Build shaded JARs
mvn clean package -DskipTests
```

**SQL parsing notes:** Calcite treats some identifiers as reserved words (`value`, `count`). Use unambiguous column names (e.g. `score` instead of `value`) or quote identifiers where needed.

---

## License

MIT — see [LICENSE](LICENSE).
