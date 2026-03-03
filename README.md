# DuckCluster

A **distributed SQL query coordinator** for analytical workloads. Clients submit SQL over a REST API; DuckCluster plans the query with Apache Calcite, executes shard-local fragments on **DuckDB worker nodes**, streams partial results over gRPC, and merges them into a final answer.

Built in Java 17 as a multi-module Maven project.

---

## Status

| Phase | Focus |
|-------|--------|
| **0** | Foundation — gRPC, worker registry, REST health, Calcite skeleton
| **1** | Scan fan-out — `POST /v1/query`, fragment execution, concatenate merge
| **2** | Pushdown + aggregation — filter/project pushdown, two-phase GROUP BY & partial agg
| **3** | ORDER BY + LIMIT — top-K merge across shards
| **4+** | Reliability, dashboard, Arrow transfer | 📋 See [roadmap](#roadmap) |

For design details, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

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

**Sharding model:** tables are registered in a cluster catalog with a shard key (demo: `id % N`). Each worker holds a disjoint subset of rows. Fragment SQL is generated in DuckDB dialect with predicates like `MOD(id, 3) = 0`.

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

This starts one coordinator (HTTP `8080`, gRPC `9090`) and three workers (`9101`–`9103`), each with a demo `events` table sharded by `id`.

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
| `DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC` | `5` | Heartbeat period |
| `DUCKCLUSTER_HEARTBEAT_MISS_THRESHOLD` | `3` | Missed beats before worker marked unhealthy |
| `DUCKCLUSTER_SHARD_COUNT` | `3` | Logical shard count for demo catalog |

---

## Project layout

```
DuckCluster/
├── proto/           # Protobuf + gRPC service definitions
├── common/          # Models, config, Calcite planner, merge interfaces
├── coordinator/     # REST API, worker registry, query execution, merger
├── worker/          # gRPC server, DuckDB JDBC fragment executor
├── scripts/         # start-cluster.sh, integration test helpers
└── docs/
    └── ARCHITECTURE.md
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

### Phase 0 — Foundation

Maven multi-module layout, protobuf/gRPC codegen, worker registration with bidirectional heartbeat, REST health endpoints, and a `QueryPlanner` interface backed by Calcite.

### Phase 1 — Scan fan-out

`POST /v1/query`, Calcite-based fragment generation with shard predicate injection, worker `ExecuteFragment` with streaming `RowBatch` responses, and concatenate merge on the coordinator.

### Phase 2 — Pushdown + aggregation

Calcite fragment SQL generation with filter/project/aggregate pushdown, partial aggregate aliases (`__dc_agg_N`), and coordinator-side merge via embedded DuckDB for `GROUP_BY_MERGE` and `PARTIAL_AGG` strategies.

Example supported today:

```sql
SELECT category, COUNT(*) AS cnt
FROM events
GROUP BY category
```

### Phase 3 — ORDER BY + LIMIT ⏳

Distributed **top-K** queries: each worker returns local top-K rows for the requested sort key; the coordinator performs a final sort and `LIMIT` in DuckDB.

Planned deliverable:

```sql
SELECT * FROM events ORDER BY value DESC LIMIT 10
```

Infrastructure already in place: `MergeStrategyType.TOP_K`, gRPC `MergeHint.TOP_K`, and planner detection for queries with `ORDER BY` / `FETCH`. The `TopKMergeStrategy` implementation is still a stub.

### Later phases

| Phase | Focus |
|-------|--------|
| **4** | Failure detection, shard reassignment, in-flight fragment retry, SSE event stream |
| **5** | HTML dashboard, broader integration tests |
| **6** | Apache Arrow transfer, broadcast join, Docker Compose, load-aware scheduler |

---

## Demo data

Each worker loads a small in-memory `events` table at startup (`id`, `name`, `value`, `category`), containing only rows where `id % shardCount == shardIndex`. This is for development and integration tests — production deployments should load pre-partitioned files or attach shared storage per worker (see architecture doc).

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
