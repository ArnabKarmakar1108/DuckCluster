# DuckCluster — Architecture & Design

**Status:** Phase 0 complete  
**Language:** Java 17  
**License:** TBD

---

## 1. Overview

DuckCluster is a **distributed SQL query coordinator** that runs analytical queries across a cluster of **DuckDB worker nodes**. Clients submit SQL over a REST API; the coordinator plans the query, partitions work across shards, collects partial results over gRPC, and merges them into a final answer.

Design goals:

- **REST-first** client interface (any language, easy to demo)
- **Apache Calcite** for SQL parsing, validation, and fragment generation
- **Embedded DuckDB (JDBC)** on each worker for local OLAP execution
- **gRPC** for typed, low-overhead coordinator↔worker communication
- **Explicit scheduling, heartbeat, and failure recovery** as first-class concerns
- **Live observability** via a lightweight dashboard (planned)

---

## 2. System Architecture

### 2.1 High-Level Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         DuckCluster                              │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │ REST API     │  │ Calcite      │  │ Scheduler              │ │
│  │ (Javalin)    │→ │ Planner      │→ │ (shard → worker map)   │ │
│  └──────────────┘  └──────────────┘  └───────────┬────────────┘ │
│         │                    │                    │              │
│         │           ┌────────▼────────┐  ┌────────▼────────┐    │
│         │           │ Query Fragment  │  │ Worker Registry │    │
│         │           │ Generator       │  │ + Heartbeat Mgr │    │
│         │           └────────┬────────┘  └────────┬────────┘    │
│         │                    │                    │              │
│  ┌──────▼────────────────────▼────────────────────▼──────────┐  │
│  │              Result Merger (+ embedded DuckDB)             │  │
│  └──────────────────────────┬────────────────────────────────┘  │
│                             │ gRPC (control + result stream)     │
└─────────────────────────────┼────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
   ┌─────────┐           ┌─────────┐           ┌─────────┐
   │ Worker 0│           │ Worker 1│           │ Worker N│
   │ DuckDB  │           │ DuckDB  │           │ DuckDB  │
   │ (JDBC)  │           │ (JDBC)  │           │ (JDBC)  │
   └─────────┘           └─────────┘           └─────────┘
```

### 2.2 Query Execution Flow

```
Client SQL
    │
    ▼
Coordinator: parse + validate (Calcite)
    │
    ├─ Detect merge strategy (scan / agg / group-by / top-k)
    ├─ Generate N fragment SQL statements with shard predicates
    ├─ Scheduler assigns fragments → workers
    │
    ├──────────┬──────────┬──────────┐
    ▼          ▼          ▼          ▼
 Worker 0   Worker 1   Worker 2   Worker N
 (partial)  (partial)  (partial)  (partial)
    │          │          │          │
    └──────────┴──────────┴──────────┘
                    │
                    ▼
         Coordinator: merge partial results
                    │
                    ▼
              JSON response
```

### 2.3 Module Layout

```
DuckCluster/
├── pom.xml                 # Maven parent (Java 17)
├── proto/                  # Protobuf + gRPC service definitions
├── common/                 # Shared models, config, planner interface
├── coordinator/            # REST API, registry, scheduler, merger
├── worker/                 # gRPC server, DuckDB JDBC executor
├── scripts/                # Cluster startup helpers
└── docs/
    └── ARCHITECTURE.md     # This document
```

### 2.4 Technology Stack

| Concern | Choice | Rationale |
|---------|--------|-----------|
| Build | Maven, Java 17 | Multi-module layout; grpc-java codegen |
| REST | Javalin 6.x | Lightweight HTTP layer |
| SQL planning | Apache Calcite + `DuckDBSqlDialect` | Parse, validate, pushdown, SQL generation |
| Worker engine | DuckDB JDBC | Embedded OLAP per worker node |
| RPC | grpc-java + protobuf | Typed messages, streaming results |
| Result transfer (v1) | Protobuf `RowBatch` | JDBC → rows → proto |
| Result transfer (v2) | Apache Arrow Java | Columnar batches (stretch) |
| Merge engine | Embedded DuckDB on coordinator | Final GROUP BY / ORDER BY |
| Dashboard | HTML + SSE | No frontend build step |
| Tests | JUnit 5 | Unit + integration coverage |

---

## 3. Goals & Non-Goals

### 3.1 Goals (v1)

1. REST API: submit SQL, receive JSON results
2. N workers with embedded DuckDB (JDBC)
3. Single-table SELECT fan-out with filter pushdown
4. Two-phase aggregation (partial agg on workers, final merge on coordinator)
5. ORDER BY + LIMIT merge (top-K per shard → coordinator merge)
6. gRPC control plane + streaming partial results
7. Scheduler with round-robin / hash shard assignment
8. Heartbeat, failure detection, and in-flight fragment retry
9. Execution dashboard (worker health, timings, shard assignments)

### 3.2 Non-Goals (v1)

- DuckDB storage extension or transparent `ATTACH DATABASE`
- Multi-table JOIN optimization (stretch goal)
- Cross-shard transactional writes
- Persistent coordinator metadata (in-memory is fine for v1)
- Authentication / multi-tenancy
- Replacing DuckDB's local optimizer on workers

---

## 4. Data Model & Sharding

### 4.1 Sharded Tables

Tables are registered with the coordinator as **logically sharded**:

```
TableShardConfig {
  tableName:   "events"
  shardKey:    "user_id"     // or implicit rowid
  shardCount:  16
  assignment:  { shard 0→worker-1, shard 1→worker-1, ... }
}
```

Each worker stores a **disjoint subset** of rows. Data can be loaded via coordinator-routed INSERT/COPY or pre-split CSV files per worker for demos.

### 4.2 Partition Predicates

| Priority | Condition | Strategy | Example |
|----------|-----------|----------|---------|
| 1 | Known row count + `rowid` | Range | `rowid BETWEEN 0 AND 99999` |
| 2 | Declared shard key | Hash modulo | `(hash(user_id) % 16) = 3` |
| 3 | Unknown cardinality | Rowid modulo | `(rowid % N) = k` |

Workers execute **fragment SQL** — DuckDB-dialect statements with injected shard predicates, generated by Calcite.

### 4.3 Coordinator DuckDB

The coordinator embeds DuckDB for:

- Final merge queries over collected partial results
- In-memory metadata catalog
- Temporary tables holding worker batches between merge steps

---

## 5. Component Design

### 5.1 REST API

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/v1/query` | Submit SQL, receive result |
| `GET` | `/v1/query/{id}` | Poll async query (future) |
| `GET` | `/v1/cluster/workers` | List registered workers + health |
| `GET` | `/v1/cluster/health` | Cluster liveness summary |
| `POST` | `/v1/admin/tables` | Register sharded table |
| `GET` | `/v1/events` | SSE feed for dashboard |

Example response:

```json
{
  "queryId": "uuid",
  "columns": ["category", "cnt"],
  "rows": [["A", 42], ["B", 17]],
  "stats": {
    "workersUsed": 4,
    "tasksGenerated": 8,
    "mergeStrategy": "GROUP_BY_MERGE",
    "durationMs": 120
  }
}
```

### 5.2 Query Planner (Calcite)

Calcite handles parse → validate → relational plan → SQL generation. A stable `QueryPlanner` interface isolates the rest of the coordinator from planner internals:

```java
interface QueryPlanner {
  PlannedQuery plan(String sql, ClusterCatalog catalog);
}

record PlannedQuery(
  List<FragmentSpec> fragments,
  MergeStrategy mergeStrategy,
  List<ColumnDef> schema
) {}
```

**Phase 0–1:** Calcite parse + RelNode analysis → merge strategy detection → `RelToSqlConverter` with `DuckDBSqlDialect` → manual shard predicate injection.

**Phase 2+:** Custom `ShardingSchema` and `ShardPredicateRule` for richer filter/project pushdown — same interface, no coordinator rewrite.

Key planned classes:

| Class | Role |
|-------|------|
| `CalciteQueryPlanner` | Parse, analyze, produce fragments |
| `DuckClusterSchema` | Calcite schema exposing sharded tables |
| `ShardPredicateRule` | Injects partition predicates into fragments |
| `MergeStrategyPlanner` | Selects CONCAT / PARTIAL_AGG / GROUP_BY / TOP_K |

Pushdown scope (v1): single table; filters; projections; aggregates (no DISTINCT); no JOIN.

### 5.3 Scheduler

Decoupled from the HTTP thread pool:

```java
interface Scheduler {
  WorkerId assignShard(int shardId);
  void onWorkerFailed(WorkerId id);
  Map<ShardId, WorkerId> currentAssignment();
}
```

v1 uses round-robin or static hash (`shardId % liveWorkers`). v2 adds load-aware placement using heartbeat load metrics.

On worker failure:

1. Heartbeat manager marks worker DEAD after N missed intervals
2. Scheduler reassigns shards to healthy workers
3. In-flight queries retry affected fragments once (idempotent reads)

### 5.4 Worker

gRPC service surface:

```
CoordinatorService (coordinator)     WorkerService (worker)
─────────────────────────────────    ─────────────────────────────
RegisterWorker                         Ping
Heartbeat (bidi stream)                ExecuteFragment → stream RowBatch
                                       GetTableStats (planned)
```

Worker internals:

- Embedded DuckDB via JDBC (single connection OK for v1)
- `FragmentExecutor`: run fragment SQL, stream row batches (~8192 rows)
- `StatsCollector`: `SELECT COUNT(*), MIN(rowid), MAX(rowid) FROM t`

### 5.5 Result Merger

| Strategy | Worker output | Coordinator action |
|----------|---------------|-------------------|
| `CONCATENATE` | Raw rows | Append batches |
| `PARTIAL_AGG` | Partial SUM/COUNT/MIN/MAX | Re-aggregate in DuckDB |
| `GROUP_BY_MERGE` | Partial grouped rows | `GROUP BY` merge in DuckDB |
| `TOP_K` | Local top-K rows | Final sort + limit in DuckDB |

Aggregate function types are tracked from the Calcite plan (not column-name heuristics) to ensure correct two-phase merge.

### 5.6 Heartbeat & Failure Detection

```
Worker                              Coordinator
  │── RegisterWorker ──────────────►│ store in registry
  │── Heartbeat (every 5s, bidi) ──►│ update lastSeen + load
  │◄── Ack ──────────────────────────│
  │                                  │
  │ (N missed heartbeats)            │
  │                       mark DEAD ─┤
  │                    reassign ─────┤
  │                    emit SSE ─────┤
```

### 5.7 Dashboard (planned)

Single page at `/dashboard/` consuming SSE from `/v1/events`:

- Worker grid: ID, status, last heartbeat, load, assigned shards
- Query timeline: queryId, fragment count, per-worker timing
- Optional Chart.js timing bars

---

## 6. Wire Protocol (`cluster.proto`)

```protobuf
service CoordinatorService {
  rpc RegisterWorker(RegisterWorkerRequest) returns (RegisterWorkerResponse);
  rpc Heartbeat(stream HeartbeatRequest) returns (stream HeartbeatResponse);
}

service WorkerService {
  rpc Ping(PingRequest) returns (PingResponse);
  rpc ExecuteFragment(FragmentRequest) returns (stream RowBatch);
}

message FragmentRequest {
  string query_id = 1;
  uint32 fragment_id = 2;
  string sql = 3;
  MergeHint merge_hint = 4;
  repeated string result_column_names = 5;
}

enum MergeHint {
  CONCAT = 1;
  PARTIAL_AGG = 2;
  GROUP_BY = 3;
  TOP_K = 4;
}
```

Phase 2 stretch: optional `arrow_ipc_bytes` field on `RowBatch` for columnar transfer.

---

## 7. Example: Distributed GROUP BY

**Query:**

```sql
SELECT category, COUNT(*)
FROM events
WHERE ts > '2024-01-01'
GROUP BY category
```

**Steps:**

1. Calcite validates query against sharded `events` catalog (8 shards, 4 workers)
2. Merge strategy → `GROUP_BY_MERGE`
3. Generate 8 fragments, e.g.:

   ```sql
   SELECT category, COUNT(*) AS cnt
   FROM events
   WHERE ts > '2024-01-01' AND (hash(user_id) % 8) = 0
   GROUP BY category
   ```

4. Scheduler assigns shards to workers; workers execute in parallel
5. Coordinator loads partial results into `temp_merge`, runs:

   ```sql
   SELECT category, SUM(cnt) FROM temp_merge GROUP BY category
   ```

6. Return merged JSON result + execution stats

---

## 8. Configuration

Environment variables (defaults shown):

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKCLUSTER_COORDINATOR_HOST` | `127.0.0.1` | Coordinator bind/connect host |
| `DUCKCLUSTER_COORDINATOR_HTTP_PORT` | `8080` | REST API port |
| `DUCKCLUSTER_COORDINATOR_GRPC_PORT` | `9090` | Worker registration port |
| `DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC` | `5` | Heartbeat period |
| `DUCKCLUSTER_HEARTBEAT_MISS_THRESHOLD` | `3` | Missed beats before DEAD |

Start a local cluster:

```bash
./scripts/start-cluster.sh
curl http://localhost:8080/v1/cluster/workers
```

---

## 9. Implementation Roadmap

Assumes ~15–20 hrs/week part-time.

### Phase 0 — Foundation ✅

- Maven multi-module layout (Java 17)
- Protobuf + gRPC codegen
- Worker registration + REST health endpoints
- Calcite parse/validate skeleton (`QueryPlanner` interface)
- `scripts/start-cluster.sh`

### Phase 1 — Scan Fan-Out (Weeks 2–4)

- `POST /v1/query`
- Calcite fragment generation with manual `rowid` shard inject
- Worker `ExecuteFragment` + JDBC + row streaming
- Coordinator CONCAT merge

### Phase 2 — Pushdown + Aggregation (Weeks 5–7)

- Custom Calcite sharding rules
- Filter/project pushdown
- Two-phase GROUP BY merge

### Phase 3 — ORDER BY + LIMIT (Week 8)

- Top-K merge strategy

### Phase 4 — Reliability (Weeks 9–10)

- Failure detection + shard reassignment
- In-flight fragment retry
- SSE event stream

### Phase 5 — Dashboard + Polish (Weeks 11–12)

- HTML dashboard, integration tests, README

### Phase 6 — Stretch

- Apache Arrow transfer, broadcast join, Docker Compose, load-aware scheduler

---

## 10. Testing Strategy

| Level | Scope |
|-------|-------|
| Unit | Fragment SQL generation, merge SQL builder, scheduler logic |
| Component | Worker executes known SQL; merger with mock batches |
| Integration | 3 workers, 100k rows, compare vs single-node DuckDB |
| Failure | Kill worker mid-query; assert retry succeeds |
| Correctness | Golden tests for COUNT/SUM/MIN/MAX, GROUP BY, TOP-K |

---

## 11. Design Trade-offs

| Decision | Benefit | Cost |
|----------|---------|------|
| External coordinator (not DuckDB extension) | REST API, portable planner, clear module boundaries | No transparent `ATTACH DATABASE` UX |
| Calcite + JDBC (not internal DuckDB plans) | Java-native planning, federated-query patterns | No row-group-aligned partitioning via storage internals |
| Protobuf rows first, Arrow later | Faster to ship; simpler debugging | Higher serialization overhead at scale |
| Single coordinator (v1) | Simpler ops | No HA failover |
| Manual shard inject before custom Calcite rules | Pipeline works end-to-end sooner | ~1 extra week before rich pushdown |

---

## 12. Open Decisions

1. **Shard key default:** implicit `rowid` vs explicit column per table?
2. **Data loading:** coordinator-routed INSERT vs offline per-worker CSV?
3. **Async queries:** blocking REST for v1, or job ID + poll from day one?
4. **Coordinator HA:** single node assumed (document as limitation)?

**Recommended defaults:** `rowid` modulo; offline CSV for demos; synchronous REST; single coordinator.

---

## 13. References

- [Apache Calcite](https://calcite.apache.org/) — federated SQL planning
- [Calcite DuckDB dialect (CALCITE-6988)](https://issues.apache.org/jira/browse/CALCITE-6988)
- [DuckDB JDBC driver](https://duckdb.org/docs/api/java)
- [Apache Arrow Java](https://arrow.apache.org/docs/java/)
- [gRPC Java](https://grpc.io/docs/languages/java/)
