# Design Decisions

This document covers the key architectural decisions, configuration knobs, and runtime behavior of DuckCluster's data locality and reliability layers (Phases A-D).

---

## 1. DuckDB Connection Pool

### Problem

DuckDB JDBC connections are **not thread-safe** — concurrent use of a single connection causes corruption. Workers need to handle multiple gRPC fragment requests simultaneously.

### Decision

Each worker maintains a bounded connection pool (`DuckDBConnectionPool`) backed by a `java.util.concurrent.ArrayBlockingQueue`.

### How it works

```
Worker startup
    │
    ▼
Open N connections to file-backed DuckDB (jdbc:duckdb:<path>)
    │
    ▼
ArrayBlockingQueue<Connection> (size = N)
    │
    ├── checkout() → poll(waitMs) → validate → return
    └── checkin()  → offer back to queue
```

- **File-backed mode** (`jdbc:duckdb:<path>`): All connections share the same database file. DuckDB handles its own internal locking via WAL. This means ATTACHed catalogs (shard files) are visible to all connections on the same database.
- **Pool exhaustion**: When all connections are in use, `checkout()` blocks up to `poolWaitMs` then throws. The gRPC handler returns `RESOURCE_EXHAUSTED` to the coordinator, which triggers the fallback path.
- **Validation**: On checkout, `isValid(1)` is called. If the connection is broken, it is replaced with a fresh one from the same JDBC URL.

### Configuration

| Variable | Default | Notes |
|----------|---------|-------|
| `DUCKCLUSTER_POOL_SIZE` | `max(2, min(4, CPUs-1))` | Bounded per worker |
| `DUCKCLUSTER_POOL_WAIT_MS` | `200` | Timeout before RESOURCE_EXHAUSTED |

### Why not HikariCP / other pool libraries?

DuckDB's embedded nature (single-process, file-locked) means standard pool features like connection eviction, leak detection, and multi-tenant isolation add complexity without benefit. The queue-based pool is ~50 lines and perfectly matches the use case.

---

## 2. Data Locality via .duckdb Shard Files

### Problem

Workers need to serve queries against partitioned data. Data must be discoverable at runtime without hardcoding table content into the application.

### Decision

Data is stored as standalone `.duckdb` files (one per shard) placed in each worker's data directory. Workers use DuckDB's `ATTACH` command to make these files accessible as named catalogs.

### File naming convention

```
<table>_shard<id>.duckdb
```

Examples: `events_shard0.duckdb`, `orders_shard3.duckdb`

Parsed by `ShardFileMetadata.fromPath()` using regex: `^([a-zA-Z_][a-zA-Z0-9_]*)_shard(\d+)\.duckdb$`

### ATTACH model

Each worker has a **main database file** (e.g. `worker-1.db`) — this is what the connection pool connects to. Shard files are separate `.duckdb` files that are made accessible inside the main database using DuckDB's `ATTACH` command:

```sql
ATTACH '/data/worker-1/events_shard0.duckdb' AS events_shard0;
-- The table inside this shard file is now queryable as: events_shard0.events
```

In DuckDB, `ATTACH` creates a named **catalog** (a namespace) within the current database session. Once attached, you reference tables inside it with dot notation: `<catalog_name>.<table_name>`.

Key property: **ATTACH on a file-backed DuckDB is database-scoped, not connection-scoped.** This means:
- The `ShardManager` executes `ATTACH` via a dedicated admin connection to the main DB file
- All pool connections to that same main DB file automatically see the attached shard catalogs
- No need to ATTACH per-connection — one ATTACH operation serves all concurrent query connections

To summarize the hierarchy:
```
worker-1.db (main database file — what the pool connects to)
├── [built-in main catalog]
├── events_shard0 (attached catalog → events_shard0.duckdb file)
│   └── events (table inside the shard file)
├── events_shard3 (attached catalog → events_shard3.duckdb file)
│   └── events (table inside the shard file)
└── ...
```

Fragment SQL uses qualified names: `SELECT * FROM "events_shard0"."events" WHERE ...`

### ShardManager

Owns the admin connection and serializes DDL:

```
ShardManager
├── adminConnection (dedicated, for ATTACH/DETACH only)
├── attachedShards: ConcurrentHashMap<catalogName, ShardFileMetadata>
├── attachShard(meta)   → ATTACH '<path>' AS <catalog>
├── detachShard(meta)   → DETACH <catalog>
└── scanAndAttachAll()  → batch-discover on startup
```

All mutation methods are `synchronized` to prevent concurrent DDL.

### Worker registration flow

```
Worker startup
    │
    ├── 1. Create ShardManager (admin connection to worker DB file)
    ├── 2. scanAndAttachAll() — find *.duckdb in dataDir, ATTACH each
    ├── 3. Create connection pool (over same DB file, sees attached catalogs)
    ├── 4. Register with coordinator, sending owned_shards list
    └── 5. Start ShardFileWatcher for ongoing discovery
```

The coordinator receives the initial shard ownership in the `RegisterWorkerRequest.owned_shards` field, populating the `ShardCatalog` immediately.

---

## 3. Consistent Hashing and Shard Routing

### Problem

When workers are added or removed, shard ownership must change minimally. A naive modulo scheme would reassign nearly all shards on topology changes.

### Decision

A **consistent hash ring** with virtual nodes (vnodes) determines which workers *should* own each shard. This gives O(1/N) shard movement on worker add/remove.

### Ring mechanics

```
ConsistentHashRing
├── TreeMap<Integer, String> ring     — position → workerId
├── Map<String, List<Integer>>        — workerId → positions
├── vnodesPerWorker = 100 (default)
└── hash: SHA-256, first 4 bytes as int
```

- Each worker gets 100 virtual node positions on the ring
- A shard key (e.g. `events:0`) is hashed to a position, then we walk clockwise to find the first N *distinct* workers
- `getOwners("events:0", replicationFactor=2)` → `["worker-2", "worker-3"]`

### Separation of routing from actual ownership

The system maintains two views:

| View | Source | Used for |
|------|--------|----------|
| **Ring-based (expected)** | `ConsistentHashRing.getOwners()` | Query routing, split-and-distribute placement |
| **Actual ownership** | `ShardCatalog.actualOwnership` map | Replication decisions |

**Why separate?** At startup, a worker hasn't loaded its shards yet. If routing depended on actual ownership, queries would fail during the registration window. Ring-based routing is deterministic and instant.

### What happens on worker add/remove

1. **Worker added** → Ring recomputes → ~1/N shards now "expected" on the new worker → ShardReplicator detects under-replication → streams shard files from existing owners to the new worker
2. **Worker removed** → Ring recomputes → shards formerly on that worker are now expected elsewhere → replication fills the gap

Both events trigger an immediate `ShardReplicator.triggerReconcile()` rather than waiting for the 30-second timer.

### Configuration

| Variable | Default | Notes |
|----------|---------|-------|
| `DUCKCLUSTER_VNODES_PER_WORKER` | `100` | More vnodes = better distribution, more memory |
| `DUCKCLUSTER_REPLICATION_FACTOR` | `2` | How many workers hold each shard |

---

## 4. ShardFileWatcher and Replication

### Watcher thread

Each worker runs a `ShardFileWatcher` — a daemon thread that polls the data directory on a fixed interval.

```
Every 2 seconds (configurable):
    scan dataDir for *.duckdb files
        │
        ├── New file found? → ShardManager.attachShard() + notify coordinator
        └── File removed?   → ShardManager.detachShard() + notify coordinator
```

The watcher uses `ShardFileMetadata.fromPath()` to parse filenames. Files that don't match the naming convention are ignored.

### Notification to coordinator

On any change, the watcher calls `coordinatorClient.updateShardOwnership(workerId, currentShards)`. This is a **full replace** — the coordinator replaces the worker's entry in `actualOwnership` with the new set.

### Replication flow

When `ShardCatalog.getUnderReplicatedShards()` finds shards with fewer actual owners than `replicationFactor`, the `ShardReplicator` orchestrates file-level streaming:

```
ShardReplicator.reconcile()
    │
    ├── Find under-replicated shards
    ├── For each: pick source (has it), pick target (ring says should have it)
    └── Stream: ReadShard(source) → ReceiveShard(target)
                  │                        │
                  ▼                        ▼
         Read .duckdb file           Write to .tmp file
         as 64KB chunks              Atomic rename on is_last
```

- Max 2 concurrent replications (semaphore-gated)
- Idempotent: if the target's watcher reports the shard before replication starts, it's skipped
- The target worker's watcher picks up the new file on its next poll cycle and sends ownership update to the coordinator

### Where does reconciliation get its data?

The `ShardReplicator` reads entirely from the **coordinator's in-memory state** — it never queries workers directly:

```
ShardReplicator.reconcile()
    │
    └── shardCatalog.getUnderReplicatedShards()
            │
            ├── Reads actualOwnership (ConcurrentHashMap on coordinator)
            │   └── Populated by worker push: registerWorker() + updateShardOwnership() RPCs
            │
            └── Compares against ring.getOwners() (expected placement)
                └── Returns shards where actual < replicationFactor
```

Workers push their ownership state to the coordinator at two points:
1. **At registration** — initial `owned_shards` in `RegisterWorkerRequest`
2. **On file changes** — `ShardFileWatcher` sends `UpdateShardOwnership` RPC whenever files appear/disappear

The coordinator never polls workers. This is a push-only model — workers are the source of truth for what they actually hold, and they push updates proactively.

### Configuration

| Variable | Default | Notes |
|----------|---------|-------|
| `DUCKCLUSTER_WATCHER_INTERVAL_MS` | `2000` | File polling frequency |
| `DUCKCLUSTER_REPLICATION_FACTOR` | `2` | Target replicas per shard |
| (hardcoded) | 30s | Reconciliation loop interval |
| (hardcoded) | 2 | Max concurrent replication streams |

---

## 5. Fragment Assignment and Fallback

### How fragments are routed to workers

```
QueryExecutionService.execute(sql)
    │
    ├── 1. Calcite plans the query → per-shard FragmentSpec[]
    ├── 2. For each shard: ring.getOwners(shardKey, RF) → [primary, replica...]
    ├── 3. Dispatch fragment to primary worker
    │       │
    │       ├── Success → collect result
    │       └── RESOURCE_EXHAUSTED → try next replica
    │
    └── 4. Merge all fragment results via selected MergeStrategy
```

### Fallback strategy

The `executeWithFallback` method implements ordered retry across replica owners:

1. Try the **primary owner** (first in ring order)
2. If it returns `RESOURCE_EXHAUSTED` (pool full), try the **next replica**
3. Continue through all replica owners in ring order
4. If all replicas are exhausted, the query fails with an error

This provides load-based failover without explicit health checks — if a worker's pool is saturated, queries naturally flow to replicas.

### Why RESOURCE_EXHAUSTED specifically?

- `UNAVAILABLE` / `DEADLINE_EXCEEDED` → the worker is down or unresponsive. Retrying immediately on another worker makes sense.
- `RESOURCE_EXHAUSTED` → the worker is alive but overloaded. The pool's bounded size acts as a natural backpressure signal.
- Other errors (e.g. SQL syntax errors) → not retried, propagated to the client.

### No speculative execution (yet)

The current design dispatches to one worker at a time, falling back sequentially. Speculative execution (dispatch to all replicas, take the first response) would improve tail latency but wastes cluster resources. This is deferred to a later phase.

---

## 6. Heartbeats and Worker Health

### Heartbeat protocol

Workers send heartbeats to the coordinator over a **bidirectional gRPC stream**:

```
Worker                              Coordinator
  │                                      │
  ├── HeartbeatRequest(workerId, load) ──►│
  │                                      ├── Update lastHeartbeatAt
  │◄── HeartbeatResponse(healthy=true) ──┤
  │                                      │
  ... repeats every heartbeatInterval ...
```

- `load` is a 0.0-1.0 value representing current pool utilization
- The stream is opened once at startup and persists for the worker's lifetime
- Uses a `ScheduledExecutorService` daemon thread

### Worker removal

The `HeartbeatMonitor` on the coordinator checks workers every `heartbeatInterval * missThreshold`:

```
HeartbeatMonitor (runs every heartbeatInterval * missThreshold)
    │
    ├── For each registered worker:
    │   └── if lastHeartbeatAt < (now - timeout): mark for removal
    │
    ├── Remove dead workers from WorkerRegistry
    ├── Remove from ShardCatalog.actualOwnership
    ├── Remove from ConsistentHashRing
    └── Trigger immediate reconciliation (ShardReplicator)
```

After removal, the ring no longer routes to the dead worker. Under-replicated shards are detected and replication fills the gap from surviving owners.

### Configuration

| Variable | Default | Notes |
|----------|---------|-------|
| `DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC` | `5` | How often workers send heartbeats |
| `DUCKCLUSTER_HEARTBEAT_MISS_THRESHOLD` | `3` | Beats missed before removal (timeout = 15s) |

### Design choice: pull vs push

We use **push-based heartbeats** (worker → coordinator) rather than coordinator polling workers because:
- Workers know immediately when they start; no coordinator scan lag
- The bidirectional stream gives the coordinator a natural place to signal unhealthy status back
- No additional gRPC service needed on the coordinator side for polling

---

## 7. Data Ingestion: split-and-distribute.sh

### Purpose

Prepares source data (CSV/Parquet) into shard files and places them on worker data directories before cluster start.

### Flow

```
split-and-distribute.sh
    │
    ├── 1. RingAssignerCli: compute shard → worker[] mapping via consistent hash ring
    ├── 2. DuckDB CLI: split source into N .duckdb files (hash partition on key column)
    ├── 3. Copy each shard file to RF worker directories per ring assignment
    └── 4. Write manifest.json with final placement
```

### Ring assignment CLI

A standalone Java CLI (`RingAssignerCli`) replicates the same `ConsistentHashRing` logic used at runtime:

```bash
java -cp common/target/classes io.duckcluster.common.routing.RingAssignerCli \
    --workers worker-1,worker-2,worker-3 \
    --table events --shards 6 --rf 2 --vnodes 100
```

Output:
```json
{
  "events_shard0": ["worker-2", "worker-3"],
  "events_shard1": ["worker-2", "worker-3"],
  ...
}
```

This guarantees that shard placement at ingestion time matches what the coordinator's ring expects at query time.

### DuckDB partitioning

```sql
CREATE TABLE events AS
SELECT * FROM read_csv_auto('source.csv')
WHERE abs(hash(id)) % 6 = 0;
```

Uses DuckDB's built-in `hash()` function for deterministic partitioning.

---

## Summary of all configuration flags

| Variable | Default | Component | Purpose |
|----------|---------|-----------|---------|
| `DUCKCLUSTER_COORDINATOR_HOST` | `127.0.0.1` | All | Coordinator address |
| `DUCKCLUSTER_COORDINATOR_GRPC_PORT` | `9090` | All | gRPC port |
| `DUCKCLUSTER_COORDINATOR_HTTP_PORT` | `8080` | Coordinator | REST API port |
| `DUCKCLUSTER_HEARTBEAT_INTERVAL_SEC` | `5` | Worker | Heartbeat frequency |
| `DUCKCLUSTER_HEARTBEAT_MISS_THRESHOLD` | `3` | Coordinator | Beats before removal |
| `DUCKCLUSTER_SHARD_COUNT` | `3` | All | Demo catalog shard count |
| `DUCKCLUSTER_DATA_DIR` | `./data` | Worker | Shard file directory |
| `DUCKCLUSTER_POOL_SIZE` | `max(2, min(4, CPUs-1))` | Worker | Connection pool size |
| `DUCKCLUSTER_POOL_WAIT_MS` | `200` | Worker | Pool checkout timeout |
| `DUCKCLUSTER_REPLICATION_FACTOR` | `2` | Coordinator | Target replicas |
| `DUCKCLUSTER_VNODES_PER_WORKER` | `100` | All | Hash ring resolution |
| `DUCKCLUSTER_WATCHER_INTERVAL_MS` | `2000` | Worker | File watcher poll rate |
