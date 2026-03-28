# DuckCluster

A **distributed SQL query coordinator** for analytical workloads. Clients submit SQL over a REST API; DuckCluster plans the query with Apache Calcite, executes shard-local fragments on **DuckDB worker nodes**, streams partial results over gRPC, and merges them into a final answer.

Built in Java 17 as a multi-module Maven project.

---

## Status

| Phase | Focus | Status |
|-------|--------|--------|
| **0** | Foundation — gRPC, worker registry, REST health, Calcite skeleton | Done |
| **1** | Scan fan-out — `POST /v1/query`, fragment execution, concatenate merge | Done |
| **2** | Pushdown + aggregation — filter/project pushdown, two-phase GROUP BY & partial agg | Done |
| **3** | ORDER BY + LIMIT — top-K merge across shards | Done |
| **A** | Connection pool — bounded DuckDB pool per worker, file-backed databases | Done |
| **B** | Data locality — consistent hash ring, shard catalog, replication factor | Done |
| **C** | Shard file watcher — dynamic discovery, ATTACH/DETACH, replication streaming | Done |
| **D** | Data ingestion tooling — split-and-distribute script, online rebalance, heartbeat monitor | Done |
| **E+** | Remote read fallback, dashboard, Arrow transfer | Planned |

For design details, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and [`docs/DESIGN-DECISIONS.md`](docs/DESIGN-DECISIONS.md).

---

## How it works

```
Client SQL
    │
    ▼
Coordinator (REST + Calcite planner)
    ├─ Detect merge strategy (scan / partial agg / group-by / top-k)
    ├─ Generate N fragment SQL with shard predicates
    └─ Dispatch fragments to workers via gRPC
           │
           ▼
    Worker 0 … Worker N  (embedded DuckDB per node)
           │
           ▼
Coordinator merges partial results (embedded DuckDB)
           │
           ▼
    JSON response
```

**Sharding model:** data is pre-split into `.duckdb` shard files (e.g. `events_shard0.duckdb`) and distributed to worker data directories. Workers auto-discover and ATTACH these files at startup. A consistent hash ring determines shard placement with configurable replication factor.

**Merge strategies:**

| Strategy | Use case | Worker | Coordinator |
|----------|----------|--------|-------------|
| `CONCATENATE` | `SELECT *`, filters | Return matching rows | Append batches |
| `PARTIAL_AGG` | `SELECT COUNT(*), SUM(x)` | Partial aggregates | Re-aggregate in DuckDB |
| `GROUP_BY_MERGE` | `SELECT k, COUNT(*) … GROUP BY k` | Partial grouped rows | Final `GROUP BY` in DuckDB |
| `TOP_K` | `ORDER BY … LIMIT n` | Local top-K per shard | Global sort + limit *(Phase 3)* |

---

## Quick start

### Prerequisites

- Java 17+
- Maven 3.8+
- `curl` (and optionally `python3` for pretty JSON)

### Build

```bash
mvn clean package
```

### Run a local cluster

```bash
./scripts/start-cluster.sh
```

This builds the project, splits demo data into shard files distributed across worker data directories, then starts one coordinator (HTTP `8080`, gRPC `9090`) and three workers (`9101`-`9103`). Requires `duckdb` CLI on PATH for shard file creation.

### Submit a query

```bash
# Simple scan with filter pushdown
curl -s -X POST http://127.0.0.1:8080/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT * FROM events WHERE id > 2"}' | python3 -m json.tool

# Distributed GROUP BY (Phase 2)
curl -s -X POST http://127.0.0.1:8080/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT category, COUNT(*) AS cnt FROM events GROUP BY category"}' | python3 -m json.tool

# Global aggregates without GROUP BY (Phase 2)
curl -s -X POST http://127.0.0.1:8080/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT COUNT(*) AS row_count, SUM(id) AS total FROM events"}' | python3 -m json.tool
```

### Cluster health

```bash
curl -s http://127.0.0.1:8080/v1/cluster/health
curl -s http://127.0.0.1:8080/v1/cluster/workers
```

### Integration tests

```bash
mvn clean test package
./scripts/test-phase1.sh   # scan fan-out
./scripts/test-phase2.sh   # GROUP BY + partial aggregation
```

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
| Tests | JUnit 5 |

---

## Roadmap

### Completed phases

**Phase 0-3** — Foundation, scan fan-out, pushdown + aggregation, ORDER BY + LIMIT. Full distributed SQL execution with filter/project/aggregate pushdown, two-phase GROUP BY, and top-K merge.

**Phase A** — Bounded connection pool per worker over file-backed DuckDB.

**Phase B** — Consistent hash ring, shard catalog, replication-factor-aware routing.

**Phase C** — Dynamic shard discovery via file watcher, ATTACH/DETACH, file-level replication streaming between workers.

**Phase D** — Data ingestion tooling (`split-and-distribute.sh`), heartbeat monitor, online rebalance on topology changes, `WorkerDemoDataLoader` deprecated from production.

### Upcoming

| Phase | Focus |
|-------|--------|
| **E** | Remote read fallback — stream shard data to non-owner workers when all owners are exhausted |
| **F** | HTML dashboard, broader integration tests, SSE event stream |
| **G** | Apache Arrow transfer, broadcast join, Docker Compose, load-aware scheduler |

---

## Demo data

`start-cluster.sh` uses `split-and-distribute.sh` to create `.duckdb` shard files from a CSV source and places them in per-worker data directories. Workers discover these at startup via their `ShardFileWatcher` and ATTACH them automatically.

---

## Development

```bash
# Run all unit tests
mvn test

# Build shaded JARs for coordinator and worker
mvn clean package -DskipTests
```

**SQL parsing notes:** Calcite treats some identifiers as reserved words (`value`, `count`). Quote them in SQL (`"value"`) or use unambiguous column names in queries.

---

## License

MIT — see [LICENSE](LICENSE).
