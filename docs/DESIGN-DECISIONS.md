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

### Broadcast pin sessions (SF10 fix)

#### Problem

JOIN fragments reference **all broadcast shard catalogs at once** via `UNION ALL` in fragment SQL.
A single Q07 fragment at SF10 with 9 shards needs up to **4 tables × 9 shards = 36** attached
catalogs (e.g. `orders_shard0` … `orders_shard8`, plus `supplier`, `customer`, `nation`).

`ensureShardsLocal()` prefetches missing shards one RPC at a time. With the original default cache
size of **5**, each new `LoadTempData` could **LRU-evict a shard loaded earlier in the same
prefetch loop**. Fragment execution then failed:

```
Catalog Error: Table with name orders does not exist!
Did you mean "orders_shard2.orders"?
```

This surfaced at SF10 as `All candidate workers failed` on Q07, Q10, Q17, and Q18.

#### Decision

**Pin cached shards for the duration of prefetch + fragment execution** on a worker. While pinned,
entries are not eligible for LRU eviction; the cache may temporarily exceed `maxShards` to hold the
full working set for one fragment.

#### How it works

```
Coordinator (per fragment attempt on worker W)
    │
    ├── BeginShardPin(W)          ← gRPC: tempShardCache.beginPinSession()
    │       └── pins all currently cached entries + any loaded during session
    │
    ├── ensureShardsLocal()       ← LoadTempData for each missing broadcast/driving shard
    │       └── loadShard() skips evicting pinned keys; may grow past maxShards
    │
    ├── executeFragment(W)        ← SQL references all prefetched catalogs
    │
    └── EndShardPin(W)            ← gRPC: tempShardCache.endPinSession()
            └── unpins; trim cache back to maxShards (evict unpinned LRU)
```

Pin session is scoped to **one fragment attempt on one worker** — `tryExecuteOnWorker` in
`executeFragmentWithRetry()` wraps prefetch and execution in `beginShardPin` / `endShardPin` in a
`finally` block so pins are always released, including on retry to another worker.

#### TempShardCache pin semantics

```
TempShardCache
├── entries: LinkedHashMap (access-order LRU)
├── pinnedKeys: Set<String>       — keys pinned during active session(s)
├── pinSessionDepth: int          — supports nested begin/end (future-proof)
│
├── beginPinSession()
│   ├── pinSessionDepth++
│   └── pinnedKeys ← all current entry keys
│
├── loadShard()
│   ├── on HIT/MISS: if pinSessionDepth > 0 → pinnedKeys.add(key)
│   └── makeRoom(): evict only unpinned entries; if all pinned, allow overflow
│
└── endPinSession()
    ├── pinSessionDepth--
    └── if depth == 0: clear pinnedKeys, trimToMax()
```

`touch(table, shardId)` still reorders LRU for coordinator routing hints but does **not** pin;
only an active pin session protects entries from eviction.

#### gRPC surface

Added to `WorkerService` in `cluster.proto`:

| RPC | Purpose |
|-----|---------|
| `BeginShardPin` | Start pin session before prefetch |
| `EndShardPin` | End pin session after fragment completes (success or failure) |

Coordinator calls via `WorkerNodeClient.beginShardPin()` / `endShardPin()`.

#### Capacity planning

| Setting | Old default | New default | Benchmark harness |
|---------|-------------|-------------|-------------------|
| `DUCKCLUSTER_CACHE_MAX_SHARDS` | `5` | `32` | `64` (via `benchmark-run.sh`) |

`maxShards` is the **steady-state** LRU cap after pin sessions end. During a pin session the cache
may hold more entries. For multi-table JOINs at high shard counts, set
`DUCKCLUSTER_CACHE_MAX_SHARDS` ≥ `(broadcast_tables × shard_count) + headroom`.

Rule of thumb for TPC-H: with 9 shards and up to 4 broadcast tables, budget **≥ 40** slots.

#### Interaction with coordinator cache view

`UpdateShardCache` still reports the full cache after each load/evict/trim. The coordinator's
`getCachedWorkers()` remains accurate for Tier-2 routing. Pin sessions are **worker-local** and
transparent to the coordinator — it only needs to call `BeginShardPin` before prefetching to the
same worker.

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

Before each attempt, missing driving shards (and broadcast shards on JOINs) are prefetched to the
target worker inside a **pin session** (`BeginShardPin` → prefetch → execute → `EndShardPin`).
Non-retryable errors fail immediately.

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
   ├── BeginShardPin(target worker)     ← pin session starts
   ├── For each unique target worker:
   │   ├── Does it already own orders_shard0? Skip.
   │   ├── Does it have orders_shard0 cached? Skip.
   │   └── Otherwise: stream from source via ReadShard → LoadTempData (fail query on error)
   └── All missing shards fetched in parallel (CompletableFuture)
   └── (pin held through step 4, released in EndShardPin after fragment)

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

**Multi-table JOINs** (e.g. Q07: `lineitem` + `orders` + `customer` + `supplier` + `nation`) require
many simultaneous attached catalogs per fragment. Pin sessions prevent LRU eviction mid-prefetch;
see §6 *Broadcast pin sessions*.

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
| `DUCKCLUSTER_CACHE_MAX_SHARDS` | `32` | Worker | TempShardCache LRU capacity (steady-state; pin sessions may exceed temporarily) |
| `DUCKCLUSTER_FRAGMENT_WAIT_MS` | `60000` | Coordinator | Max wait for owner/cache worker or worker capacity slot |

---

## 11. Query planner

Planner-specific decisions (Calcite scope, merge strategies, derived tables, nested `GROUP BY`,
TPC-H query status) live in **[DESIGN-planner.md](DESIGN-planner.md)**. That document is the
canonical place to update when planner behavior changes.

High-level summary: Calcite is used today as **parser + AST + SQL printer**; shard routing,
fragment rewrite, and coordinator merge are **custom logic** on `SqlNode`, not `RelNode`
optimizer rules.

---

## 12. Data structures & concurrency

### Connection pools

Both worker and coordinator use `ArrayBlockingQueue<Connection>` — a bounded, thread-safe FIFO queue — as the pool backing store. HikariCP was rejected because DuckDB JDBC doesn't support standard validation queries (`SELECT 1`) or connection metadata, and the pool logic is only ~60 lines.

| Pool | Class | Location | Size | Behavior |
|------|-------|----------|------|----------|
| Worker | `DuckDBConnectionPool` | `worker/.../duckdb/` | `DUCKCLUSTER_POOL_SIZE` (default 2–4) | `pool.poll(waitMs, MILLISECONDS)` blocks; returns `RESOURCE_EXHAUSTED` on timeout |
| Coordinator | `CoordinatorDuckDbPool` | `coordinator/.../merger/` | 4 (singleton) | Same blocking poll; `resetCatalog()` drops all tables on checkin; `Lease` inner class (AutoCloseable) for RAII |

Both use `private volatile boolean closed` for cheap cross-thread shutdown visibility without locking.

### Consistent hash ring

`ConsistentHashRing` uses a `TreeMap<Integer, String>` mapping hash positions → worker IDs. SHA-256 on `workerId + "#" + vnodeIndex` (first 4 bytes as int) generates `DUCKCLUSTER_VNODES_PER_WORKER` (default 100) virtual nodes per worker. A `ReentrantReadWriteLock` separates the hot path (`getOwners()` — many concurrent routing reads during query dispatch) from rare topology mutations (`addWorker`/`removeWorker`). `getOwners(key, count)` walks clockwise via `tailMap()` + wrap-around, collecting distinct workers up to the replication factor.

### TempShardCache (LRU + pin sessions)

`TempShardCache` is a bounded LRU cache of remotely-fetched broadcast shard DuckDB files on each worker. Backed by `LinkedHashMap<String, CacheEntry>` with `accessOrder=true` — Java's built-in LRU iteration order. All public methods are `synchronized` (simple monitor; low contention since access is per-query, not per-row).

The **pin session** mechanism prevents cache eviction during active query execution. Without it, fragment B's prefetch could evict a shard that fragment A (same query, same worker) is still reading — the root cause of SF10 Q10/Q18 failures. `beginPinSession()` increments a reentrant counter and snapshots all current cache keys into `pinnedKeys`; eviction skips pinned entries. `endPinSession()` decrements and clears pins at depth zero.

### Fragment dispatch (scatter-gather)

`QueryExecutionService` dispatches all fragment executions as `CompletableFuture.supplyAsync(runFragment, fragmentExecutor)` on a cached thread pool (unbounded, daemon threads). Results are collected into a list and joined sequentially. The same pattern handles broadcast shard prefetch (`CompletableFuture.runAsync`). This is a classic scatter-gather: fan-out is parallel and non-blocking, fan-in blocks the coordinator HTTP thread until all fragments complete.

### Worker fragment tracker

`WorkerFragmentTracker` implements per-worker capacity limiting to prevent overloading a single worker with too many concurrent fragments. Uses `ConcurrentHashMap<String, AtomicInteger>` keyed by worker ID. The `tryAcquire()` method uses `synchronized(count)` on the per-worker AtomicInteger to atomically check capacity and increment — a lightweight per-worker semaphore. `acquireBlocking()` polls with 50ms sleep until capacity is available or timeout expires.

### gRPC channel pool

`WorkerChannelPool` maintains one `ManagedChannel` per known worker via `ConcurrentHashMap.computeIfAbsent()` — thread-safe lazy initialization with no external lock. Channels are long-lived (connection pooling within gRPC). On worker death, `removeChannel()` calls `shutdown()` with a 5-second grace period followed by `shutdownNow()`.

### Worker registry

`WorkerRegistry` uses a plain `ConcurrentHashMap<String, WorkerRecord>` — lock-free concurrent register/heartbeat-update/remove/list from multiple gRPC handler threads and the heartbeat monitor. No additional synchronization needed; the map's atomicity guarantees suffice for the read-heavy workload.

### Replication throttle

`ShardReplicator` limits concurrent cross-worker shard copies to 2 via `Semaphore(MAX_CONCURRENT_REPLICATIONS)`. The reconciliation loop runs on a `ScheduledExecutorService`; each replication task calls `tryAcquire()` (non-blocking) before starting a streaming gRPC copy. This prevents network saturation during rebalancing while allowing the scheduler to continue discovering under-replicated shards.

### Streaming gRPC → blocking conversion

`WorkerNodeClient` converts asynchronous streaming gRPC responses into blocking calls using `CountDownLatch(1)` + `AtomicBoolean`. The response observer calls `latch.countDown()` on `onCompleted()` or `onError()`; the caller blocks on `latch.await(60, SECONDS)`. Used for shard push (`pushShardTo`) and temp data load (`loadTempData`).

### Scheduled daemons

| Daemon | Thread name | Interval | Purpose |
|--------|-------------|----------|---------|
| `HeartbeatMonitor` | `heartbeat-monitor` | `HEARTBEAT_INTERVAL_SEC` × `MISS_THRESHOLD` | Remove dead workers; trigger shard reconciliation |
| `ShardFileWatcher` | `shard-file-watcher` | `WATCHER_INTERVAL_MS` (2s) | Poll data directory for new/removed .duckdb files; attach/detach |
| `CoordinatorClient` | `worker-heartbeat` | `HEARTBEAT_INTERVAL_SEC` (5s) | Send heartbeat RPCs from worker to coordinator |
| `ShardReplicator` | `shard-replicator` | 10s | Reconcile under-replicated shards; copy to targets |

All use single-thread `ScheduledExecutorService` with daemon threads (JVM exits cleanly without explicit shutdown).

---

## 13. Design goals & non-goals

### Goals (v1)

- REST SQL API with synchronous JSON response
- N-worker horizontal fan-out with gRPC fragment dispatch
- Two-phase distributed aggregation (PARTIAL_AGG, GROUP_BY_MERGE)
- ORDER BY + LIMIT merge (TOP_K with per-fragment oversample)
- gRPC control plane with push-based heartbeat failure detection
- Shard replication via consistent hash ring with configurable RF
- Data locality — each worker reads only its local `.duckdb` shard files
- Single planner pipeline — no "simple vs advanced" modes; every query follows the same path

### Non-goals

- **No storage engine extension** — uses DuckDB as-is via JDBC; no C++ plugin or custom catalog
- **No cross-shard writes / distributed transactions** — read-only analytical workloads only
- **No persistent metadata store** — in-memory registry rebuilt on restart from worker shard reports
- **No authentication / multi-tenancy** — single-user, no auth layer
- **No custom wire format** — Protobuf string columns today; typed Arrow IPC is future work
- **No dynamic query routing** — all queries go through one coordinator; no multi-coordinator HA

---

## 14. Challenges & solutions

Chronological record of the major engineering challenges encountered during development, their root causes, and the solutions implemented. This section preserves the detailed optimization history.

### Challenge 1: Coordinator merge stall — row-by-row INSERT

**Problem:** At SF1, queries Q13/Q15/Q16 stalled for **minutes** with coordinator CPU at ~500%. Workers finished in tens of milliseconds but the coordinator couldn't load ~900k partial-aggregate rows into its temp table. Root cause: one `INSERT INTO … VALUES (…)` per row — O(rows) SQL parse/plan/execute round-trips inside coordinator DuckDB.

```java
// Before: O(rows) round-trips
for (List<Object> row : rows) {
    statement.execute("INSERT INTO __merge_temp (...) VALUES (...)");
}
```

**Solution:** `DuckDbBulkInserter` — primary path uses DuckDB's native `DuckDBAppender` API (columnar bulk load, bypasses SQL entirely); fallback uses batched multi-row INSERT (512 rows per statement) for JDBC drivers without Appender support.

**Measurements:** Micro-benchmark: 10k rows — batched ~957ms vs Appender ~40ms (~24× faster). Q13: minutes → **1226ms**. Q16: minutes → **699ms**. All 22 queries complete at SF1 in one pass.

---

### Challenge 2: Java heap ping-pong in two-phase merge

**Problem:** Nested (`NESTED_GROUP_BY_MERGE`) and CTE (`WITH_CTE_MERGE`) strategies required two phases. Phase-1 output was read from DuckDB into Java (`ResultSet → List<List<Object>>`), temp table dropped, then re-inserted via Appender for phase-2. This DuckDB→Java→DuckDB round-trip added heap pressure and latency.

```
fragment rows → __merge_temp → phase-1 SQL → readRows() into Java heap
  → DROP __merge_temp → bulk re-insert → phase-2 SQL
```

**Solution:** CTAS-based in-DB handoff keeps all intermediate data inside DuckDB:

```sql
CREATE TABLE __merge_phase1 AS (<phase1Sql>);
DROP TABLE __merge_temp;
ALTER TABLE __merge_phase1 RENAME TO __merge_temp;
-- phase-2 SQL runs directly on __merge_temp
```

**Measurements:** Q15: 210ms → **136ms** (−35%). Q13: 1226ms → **1076ms** (−12%).

---

### Challenge 3: Fragment result materialization overhead

**Problem:** `collectRows()` walked every gRPC `RowBatchData` and built `List<List<Object>>` in memory before any insert. At scale this meant 3–4 heap copies per fragment result (proto → RowBatchData → collectRows list → Appender input), causing high GC pressure on large partial-agg merges (Q01, Q16).

**Solution:** Streaming Appender — `appendFragmentBatches()` projects each batch row directly into `DuckDBAppender` as it arrives from gRPC, with no intermediate List materialization. Each row is decoded from proto column values and immediately appended to the columnar buffer.

**Measurements:** Q16: 699ms → **385ms** (−45%). Coordinator heap high-water mark reduced across all queries.

---

### Challenge 4: Coordinator DuckDB connection contention

**Problem:** Every merge opened a fresh in-memory `jdbc:duckdb:` connection, ran DDL + inserts + merge SQL + DROP, then closed. Under concurrent clients (c=4), N queries spawned N simultaneous in-memory DuckDB instances with unbounded resource use — 2.5× latency degradation.

**Solution:** `CoordinatorDuckDbPool` — bounded ArrayBlockingQueue pool (size 4). `resetCatalog()` drops all tables on check-in for clean reuse. `Lease` (AutoCloseable) for RAII checkout/checkin. Singleton instance.

**Measurements:** Bounded resource use at c≥4. Latency still 2.5× worse than c=1 — pool prevents OOM but does not parallelize merge (requires future parallel merge executor).

---

### Challenge 5: Merge pushdown — flat fan-in explosion

**Problem:** All N fragment result sets loaded into one monolithic `__merge_temp`, then one global `GROUP BY` merge SQL ran over millions of rows. Q03 at SF1: all partial groups from every shard (~millions of rows) loaded before applying `ORDER BY LIMIT 10`. Workers finished in <1s; coordinator merge took multi-seconds.

**Solution:** Two-part pushdown via `MergePushdownPlanner`:

1. **Hierarchical merge** — when a worker ran multiple fragments, collapse them with an intermediate `GROUP BY` on the coordinator before the final merge. 6 fragments on 3 workers → 3 worker-summary loads → final merge on reduced data.

2. **TOP-K oversample** — for `GROUP BY + ORDER BY LIMIT` on decomposable aggregates (`SUM`/`COUNT`, not `COUNT(DISTINCT)`), each fragment SQL gets `ORDER BY … LIMIT <limit × shard_count>`. Coordinator merges a bounded subset, not the full cross-product.

**Measurements:** Suite SF1: **15.5s → 6.6s (−57%)**. Q03: 2306ms → **226ms** (−90%). Q07: 2926ms → **295ms** (−90%). Suite ratio: 7.2× → **3.2×** vs single-node.

---

### Challenge 6: SF10 worker fragment failures — cache eviction mid-query

**Problem:** At SF10, queries Q10 and Q18 failed with `RESOURCE_EXHAUSTED` on workers. Root cause: the `TempShardCache` (LRU, bounded at 32 entries) evicted `orders_shard0` while another fragment on the same worker was actively reading it. Sequence: broadcast prefetch loads shard → fragment A starts using it → fragment B's prefetch fills cache → LRU evicts shard (detaches from DuckDB catalog) → fragment C (or A's next scan batch) fails with "catalog entry not found."

**Solution:** Pin sessions in `TempShardCache`:
- `beginPinSession()` increments reentrant depth counter; snapshots all current cache keys into `pinnedKeys` HashSet
- LRU eviction logic skips entries in `pinnedKeys`
- `endPinSession()` decrements counter; clears pins at depth zero
- `QueryExecutionService` wraps the entire broadcast-prefetch + fragment-execution window in a pin session

**Measurements:** SF10: 20/22 → **22/22 OK**. No eviction failures through SF40 (56GB, 12 shards, 64-entry cache).

---

### Challenge 7: Planner correctness — 0/22 to 22/22 TPC-H queries

**Problem:** Initial smoke run on SF0.01 TPC-H data: **0/17** queries produced correct results. The planner had been tested only on a single `events` table with simple `SUM(score)` / `AVG(score)` patterns. TPC-H grammar exposed 8 classes of failure:

| ID | Failure pattern | Example queries |
|----|-----------------|-----------------|
| E1 | Expression operands in aggregates (`SUM(a * (1 - b))`) | Q01, Q03, Q05, Q06, Q10, Q12, Q19 |
| E2 | Derived tables in FROM (`FROM (SELECT …) AS alias`) | Q07, Q08, Q09, Q13 |
| E3 | Calcite reserved words in aliases | Q11 |
| E4 | Multi-aggregate arithmetic (`SUM(a)/SUM(b)`) in one SELECT item | Q14 |
| E5 | Implicit comma-joins with unqualified columns | Q05, Q10, Q12, Q14 |
| E6 | Merge SQL column name mismatch after rewrite | Q14 |
| E7 | Correlated/semi-join subqueries in WHERE | Q02, Q04, Q15, Q17, Q18, Q20, Q21, Q22 |
| E8 | `COUNT(DISTINCT …)` decomposition | Q16 |

**Solution:** Systematic 10-phase hardening of the single planner pipeline (one code path for all queries — no "simple vs advanced" branching):

1. **Expression-aware aggregate analysis** — `AggregateSpec.inputColumn` becomes nullable; added `inputExpression` field for complex operands like `l_extendedprice * (1 - l_discount)`
2. **Recursive FROM-clause traversal** — `collectTableNamesRecursive()` descends into `SqlSelect` nodes inside `FROM` clauses and JOIN trees to discover all base tables
3. **Implicit join handling** — table-qualified column names in fragment SQL; alias resolution for comma-separated multi-table FROM
4. **Multi-aggregate arithmetic decomposition** — `SUM(a)/SUM(b)` split into individual `__dc_agg_N` merge columns; final expression reconstructed in coordinator merge SQL
5. **Parser edge cases** — Calcite `SqlParser.Config` adjusted for reserved word handling; deterministic alias generation
6. **COUNT(DISTINCT) merge** — coordinator receives concatenated raw values and applies `COUNT(DISTINCT ...)` on the full set (cannot merge partial distinct counts)
7. **Correlated subquery pass-through** — predicates referencing outer-scope tables (`WHERE l_orderkey = o_orderkey`) preserved verbatim in fragment SQL; workers execute them against locally-attached broadcast shards
8. **TOP_K on joined queries** — oversample with shard-count multiplier on fragment SQL; coordinator applies final `ORDER BY LIMIT`
9. **Execution hardening** — actionable error messages distinguishing planning vs worker vs merge failures; gRPC timeout handling; retryable status codes
10. **Full SF0.01 correctness gate** — `test_tpch.py` integration test compares DuckCluster output against single-node DuckDB baseline for all 22 queries

**Result:** **22/22** TPC-H queries pass correctness verification at SF0.01. All merge strategies work end-to-end. The integration test gate runs on every code change.

---

### Challenge 8: Node stability at scale

**Problem:** At SF40+ with sustained benchmark load, workers crash and SSH connections to the development VM hang. The 3-worker + coordinator setup shares one machine's resources, and at 56GB+ dataset sizes the OS cgroup limits (though `nproc` reports 32 vCPU / `/proc/meminfo` reports 503 GiB) become binding.

**Status:** Open — blocking SF100 benchmarks. Workaround: shorter benchmark runs (warmup=2, measured=3), fewer iterations. The measured SF10→SF20→SF40 trend data is sufficient to demonstrate the scaling dynamics (DC sub-linear 1.57× vs SN super-linear 2.36× per 2× data).

---

### Summary: optimization trajectory

| # | Challenge | Bottleneck | Key metric |
|---|-----------|------------|------------|
| 1 | Row-by-row INSERT | O(rows) SQL round-trips | Q13/Q16: minutes → ~1s |
| 2 | Java heap ping-pong | DuckDB→Java→DuckDB copy | Q15: −35% |
| 3 | Materialization overhead | Intermediate List allocation | Q16: −45% |
| 4 | Connection contention | Unbounded DuckDB instances | Resource bounded at c≥4 |
| 5 | Flat fan-in | Global GROUP BY on all partials | Suite: −57%; Q03/Q07: −90% |
| 6 | Cache eviction mid-query | LRU evicts active shard | SF10: 20/22 → 22/22 |
| 7 | Planner correctness | 8 failure classes | 0/22 → 22/22 TPC-H |
| 8 | Node stability | Cgroup pressure at SF40+ | Open |

**Suite trajectory (SF1 c=1):** pre-fix minutes+ → bulk **19.2s** → hardened **15.5s** (7.2×) → pushdown **6.6s** (3.2×).
