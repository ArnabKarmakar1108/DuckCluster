# Design Decisions

This document covers the key architectural decisions, configuration knobs, and runtime behavior of DuckCluster's data locality, reliability, dispatch, and join execution layers.

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

## 5. Parallel Dispatch and Persistent gRPC Channels

### Problem

Sequential fragment dispatch (one at a time, waiting for each to finish) severely limits throughput — a query with 6 fragments across 3 workers would take 6x the latency of a single fragment instead of ~2x.

Creating a new gRPC channel per RPC call added connection setup overhead to every fragment execution.

### Decision

- **Parallel dispatch:** All fragments are submitted concurrently via `CompletableFuture.supplyAsync()` on a cached thread pool with daemon threads.
- **Persistent channel pool:** `WorkerChannelPool` maintains one `ManagedChannel` per worker (keyed by workerId), reused across all queries for the lifetime of the worker's registration.

### How it works

```
QueryExecutionService.execute(sql)
    │
    ├── Plan query → FragmentSpec[]
    ├── Resolve target worker for each fragment
    ├── Prefetch broadcast shards (if JOIN)
    └── CompletableFuture.supplyAsync() for each fragment
            │ (all run in parallel)
            ▼
    WorkerNodeClient.executeFragment(worker, fragment)
        └── channelPool.getChannel(worker)  ← reused ManagedChannel
```

Channels are removed from the pool when:
- A worker is evicted by the `HeartbeatMonitor` (missed heartbeats)
- A worker **re-registers** with the same `workerId` (`registerWorker` calls `removeChannel` first, so a fast restart on a new port gets a fresh channel)

All channels are shut down gracefully on coordinator shutdown.

### Configuration

| Variable | Default | Notes |
|----------|---------|-------|
| (none) | — | Channel pool has no dedicated env vars; lifecycle tied to worker registration |

---

## 6. Fragment Routing, Prefetch, and Waiting

### Problem

The target worker for a fragment may not own the shard, may be temporarily at capacity, or (for JOINs) broadcast data may not yet be local. The system must route work correctly without silent data loss.

### Decision

Fragment routing is **split by query shape** (single-table vs JOIN). Prefetch always uses `ReadShard` → `LoadTempData` (temp cache, not permanent ownership). Prefetch failure **fails the query** (no log-and-continue).

### Routing tiers

```
For each driving-table fragment (shard N):
    │
    ├── Tier 1: Ring owners (primary + replicas)
    │   └── ring.getOwners(table:N, RF) — first registered worker wins
    │
    ├── Tier 2: Workers with cached copy
    │   └── shardCatalog.getCachedWorkers(table, N)
    │       (populated by TempShardCache → UpdateShardCache RPC)
    │
    └── Tier 3 (single-table queries only): Remote read fallback
        └── Pick least-loaded registered worker, prefetch driving shard via LoadTempData
```

**JOIN queries do not use Tier 3.** If no registered owner or cache holder exists for a driving shard, the coordinator **blocks up to `DUCKCLUSTER_FRAGMENT_WAIT_MS`** (polling every 100ms) for one to appear — e.g. after replication or worker registration. If the timeout expires, the query fails.

**Single-table queries** use Tier 3 when Tiers 1–2 find no registered worker. The driving shard is streamed to the fallback worker before execution (same machinery as broadcast prefetch).

### Prefetch phases

| Query type | What gets prefetched | When |
|------------|---------------------|------|
| **JOIN** | All broadcast-table shards missing on each target worker | After target resolution, before dispatch |
| **Single-table** | Driving shards missing on fallback (Tier 3) workers | After target resolution, before dispatch |

Prefetch skips shards the target already owns (`actualOwnership`) or has cached (`shardCache`). Each missing shard is fetched in parallel via `CompletableFuture`.

On prefetch failure (`loadTempData` returns false or gRPC error), the query throws `IllegalStateException` immediately.

### Worker capacity waiting

`WorkerFragmentTracker` limits concurrent fragments per worker to `numThreads` (from registration). Before dispatching a fragment, the coordinator calls `acquireBlocking(workerId, fragmentWaitMs)` — if the worker is at capacity, the query thread waits (same timeout as join worker resolution) rather than failing immediately.

This applies to **all** queries, so a JOIN routed to a busy owner waits for a free slot.

### TempShardCache and coordinator cache consistency

When a shard is streamed via `LoadTempData`, it lands in the worker's LRU `TempShardCache` (`.cache/` directory, invisible to `ShardFileWatcher`). The worker notifies the coordinator via `UpdateShardCache` (full-replace semantics).

**On LRU eviction**, the worker also calls `notifyCoordinator()` so the coordinator drops the evicted entry. Without this, `getCachedWorkers()` would stay stale and prefetch would be skipped incorrectly.

On cache hit during `loadShard`, the incoming temp file is deleted to avoid leaking orphan files.

### gRPC streaming correctness

`ReceiveShard` and `LoadTempData` handlers require a chunk with `is_last=true` before accepting success. Inbound stream errors complete the response observer with `INTERNAL`; partial temp files are cleaned up. This prevents false `accepted=true` responses after write failures.

### Why resolve before dispatch?

Target resolution happens in `resolveFragmentTargets()` before prefetch and dispatch. This lets the coordinator know which workers need broadcast or driving shards streamed before fragments run.

### Execution-time retry

If a fragment fails on the chosen worker with a retryable gRPC status (`RESOURCE_EXHAUSTED`, `FAILED_PRECONDITION`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`), the coordinator tries the next candidate in order:

1. Preferred worker from initial resolution
2. Ring owners and actual owners (registered)
3. Cached workers (registered)
4. Other registered workers (single-table queries only)

Before each attempt, missing driving shards (and broadcast shards on JOINs) are prefetched to the target worker. Non-retryable errors fail immediately.

---

## 7. JOIN Execution: Broadcast Shuffle

### Problem

Tables are sharded independently via `split-and-distribute.sh` — each table gets its own shard count, shard key, and ring placement. Two tables sharded independently will almost never have matching shard assignments (`hash("lineitem_shard0")` and `hash("orders_shard0")` land on different workers). JOINs must produce correct results without requiring coordinated placement at ingestion time.

### Decision

**Broadcast the smaller side.** For a join involving multiple sharded tables:
1. The table with the most shards becomes the **driving table** (its shard count determines fragment count)
2. All other sharded tables become **broadcast tables** (all their shards are prefetched to execution workers)
3. Fragment SQL uses `UNION ALL` of all broadcast shards so every row is visible to every driving-table fragment

### How it works

```
Query: SELECT ... FROM lineitem(6 shards) l JOIN orders(4 shards) o ON ...

1. PLAN
   ├── Driving table: lineitem (most shards → 6 fragments)
   ├── Broadcast table: orders (4 shards, UNION ALL in fragment SQL)
   └── Fragment 0 SQL:
         SELECT ... FROM "lineitem_shard0"."lineitem" AS "l"
         INNER JOIN (
           SELECT * FROM "orders_shard0"."orders"
           UNION ALL SELECT * FROM "orders_shard1"."orders"
           UNION ALL SELECT * FROM "orders_shard2"."orders"
           UNION ALL SELECT * FROM "orders_shard3"."orders"
         ) AS "o" ON ...

2. RESOLVE TARGETS
   └── For each of the 6 fragments, pick a worker owning that lineitem shard

3. PREFETCH (broadcast only — driving shards assumed local on owner/cache)
   ├── For each unique target worker:
   │   ├── Does it already own orders_shard0? Skip.
   │   ├── Does it have orders_shard0 cached? Skip.
   │   └── Otherwise: stream from source via ReadShard → LoadTempData (fail query on error)
   └── All missing shards fetched in parallel (CompletableFuture)

3b. WAIT (if no owner/cache registered for a driving shard)
   └── Block up to DUCKCLUSTER_FRAGMENT_WAIT_MS for replication/registration

4. DISPATCH
   └── Acquire worker capacity slot, send fragments (parallel)

5. MERGE
   └── Standard merge strategy (GROUP_BY_MERGE, PARTIAL_AGG, etc.)
```

### Why broadcast instead of hash-repartition?

True hash-repartition (read rows, re-hash on join key, redistribute by bucket) requires streaming individual rows at query time — significant infrastructure for row-level shuffling. Broadcasting entire shard files is simpler and leverages existing infrastructure (`ReadShard` + `LoadTempData` + `TempShardCache`).

The tradeoff: broadcasting moves more data when both tables are large. But shard files are cached across queries (`TempShardCache`), so the transfer cost is paid once. For the common pattern (large fact table + smaller dimension-like table), broadcast is efficient.

### Dynamic table classification

There are no hardcoded table lists, no "fact" vs "dimension" env vars, no user-provided classifications. The system derives everything from worker shard reports:

- Workers report `ShardOwnership(tableName, shardId)` at startup
- The coordinator's `ClusterCatalog` accumulates all known tables and their shard counts
- At query time, all tables in the FROM clause must be in the catalog (all tables are sharded)
- The planner picks the driving table by shard count, broadcasts the rest

### Multiple broadcast tables

For 3+ table joins (e.g. `lineitem JOIN orders JOIN customer`):
- lineitem (most shards) drives
- orders AND customer are both broadcast
- Each target worker receives all shards of all broadcast tables
- Fragment SQL has nested UNION ALL subqueries for each broadcast table

### No placement constraints

`split-and-distribute.sh` runs independently per table with no awareness of what queries will be executed. Different tables can have different shard counts, different shard keys, and different worker placements. The coordinator handles everything at query time.

---

## 8. Heartbeats and Worker Health

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

## 9. Data Ingestion: split-and-distribute.sh

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

## 10. Distributed Merge Correctness

### AVG

`AVG(col)` is decomposed at the fragment into `SUM(col)` + `COUNT(col)` partial aggregates. The coordinator merge computes `SUM(partial_sums) / SUM(partial_counts)` — never `AVG(partial_avgs)`.

### TOP_K

Queries with `ORDER BY` and/or `LIMIT` (and no `GROUP BY`/bare aggregates) use `TOP_K` merge strategy. Each fragment runs local `ORDER BY ... LIMIT K`; the coordinator loads partial rows into DuckDB and applies a global `ORDER BY ... LIMIT K`.

### Supported merge strategies

| Strategy | When | Merge |
|----------|------|-------|
| `CONCATENATE` | Simple `SELECT` | Row append |
| `PARTIAL_AGG` | Aggregates without `GROUP BY` | Sum/count/min/max merge |
| `GROUP_BY_MERGE` | `GROUP BY` + aggregates | Re-group and merge partials |
| `NESTED_GROUP_BY_MERGE` | Derived table with inner `GROUP BY` | Two-phase: merge inner groups, then outer agg on coordinator |
| `TOP_K` | `ORDER BY` / `LIMIT` | Global sort + limit |

See [DESIGN-planner.md](DESIGN-planner.md) for planner rationale.

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
| `DUCKCLUSTER_CACHE_MAX_SHARDS` | `5` | Worker | TempShardCache LRU capacity |
| `DUCKCLUSTER_FRAGMENT_WAIT_MS` | `60000` | Coordinator | Max wait for owner/cache worker or worker capacity slot |

---

## 11. Query planner

Planner-specific decisions (Calcite scope, merge strategies, derived tables, nested `GROUP BY`,
TPC-H query status) live in **[DESIGN-planner.md](DESIGN-planner.md)**. That document is the
canonical place to update when planner behavior changes.

High-level summary: Calcite is used today as **parser + AST + SQL printer**; shard routing,
fragment rewrite, and coordinator merge are **custom logic** on `SqlNode`, not `RelNode`
optimizer rules.
