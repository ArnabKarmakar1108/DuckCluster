# Proposal: Connection Pooling & Data Locality for DuckCluster

**Date:** 2026-07-07  
**Status:** Draft  
**Scope:** Worker-side connection safety (Phase 3 prerequisite) + data loading architecture

---

## Problem 1: DuckDB Connection Safety Under Concurrent Fragment Execution

### Current State

Each worker creates a **single** `java.sql.Connection` at startup (`WorkerDemoDataLoader.openInMemoryConnection()`) and passes it to `FragmentExecutor`. Every gRPC `ExecuteFragment` call uses that same connection object.

gRPC-java's default `ServerBuilder` dispatches incoming RPCs on a cached thread pool. Today the coordinator sends fragments sequentially, so only one thread touches the connection at a time. But this is a **latent bug**:

- Two concurrent HTTP requests to the coordinator → two threads dispatch fragments to the same worker simultaneously.
- Phase 3+ parallel dispatch will intentionally send overlapping fragments to the same worker.
- The DuckDB JDBC driver wraps a native C++ library; concurrent access to one connection causes **undefined behavior** (wrong results, segfaults, JVM crashes) — not just a Java-level exception.

The reference project (`duckdb-distributed-execution`) has the same limitation: one connection per worker, sequential execution, with "connection pooling" listed as a roadmap item.

### Proposed Solutions



#### Option A: Per-Worker Connection Pool (Recommended)

Maintain a bounded pool of DuckDB connections on each worker. Each `ExecuteFragment` call checks out a connection, executes, and returns it.

```
┌─────────────────────────────────────────────────┐
│  Worker Process                                  │
│                                                  │
│  gRPC Thread Pool                                │
│    ┌──────┐ ┌──────┐ ┌──────┐                   │
│    │ RPC1 │ │ RPC2 │ │ RPC3 │  ...              │
│    └──┬───┘ └──┬───┘ └──┬───┘                   │
│       │        │        │                        │
│       ▼        ▼        ▼                        │
│  ┌──────────────────────────────────┐            │
│  │     Connection Pool (size N)     │            │
│  │  ┌──────┐ ┌──────┐ ┌──────┐    │            │
│  │  │Conn 1│ │Conn 2│ │Conn N│    │            │
│  │  └──────┘ └──────┘ └──────┘    │            │
│  └──────────────────────────────────┘            │
│                  │                               │
│                  ▼                               │
│         ┌──────────────┐                         │
│         │  DuckDB      │                         │
│         │  (shared DB) │                         │
│         └──────────────┘                         │
└─────────────────────────────────────────────────┘
```

**Key insight:** Multiple JDBC connections to the **same** DuckDB database are safe — DuckDB handles internal locking between connections. What's unsafe is sharing a single `Connection` object across threads.

**Implementation sketch:**

```java
public class DuckDBConnectionPool implements AutoCloseable {
    private final String dbPath;          // ":memory:" or file path
    private final BlockingQueue<Connection> pool;
    private final int maxSize;

    public DuckDBConnectionPool(String dbPath, int maxSize) {
        this.dbPath = dbPath;
        this.maxSize = maxSize;
        this.pool = new ArrayBlockingQueue<>(maxSize);
        // Pre-create connections pointing to the same DB
        for (int i = 0; i < maxSize; i++) {
            pool.add(DriverManager.getConnection("jdbc:duckdb:" + dbPath));
        }
    }

    public Connection checkout(long timeoutMs) throws InterruptedException {
        return pool.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public void checkin(Connection conn) {
        pool.offer(conn);
    }
}
```

**Why not HikariCP?** DuckDB's JDBC driver doesn't support standard `DataSource`-based pool integration cleanly (connection strings differ from typical RDBMS). A purpose-built pool with 50 lines is simpler and avoids the dependency.

**Tradeoffs:**


| Aspect      | Benefit                                       | Cost                                                                           |
| ----------- | --------------------------------------------- | ------------------------------------------------------------------------------ |
| Correctness | Eliminates data races on Connection           | None — strictly required                                                       |
| Concurrency | N fragments execute in parallel on one worker | Higher memory (each connection holds its own transaction state)                |
| Complexity  | Thin wrapper (~80 LOC)                        | Must handle connection health checks, timeout on checkout                      |
| Pool sizing | Bounds worker load (backpressure)             | Over-provisioned pool wastes memory; under-provisioned pool starves throughput |


**Recommended pool size:** `max(2, Runtime.getRuntime().availableProcessors() - 1)`. DuckDB's internal parallelism already uses multiple cores per query, so 2–4 connections is typically optimal; more connections saturate CPU without improving throughput.

#### Option B: Serialize All Fragment Execution (Mutex)

Wrap `FragmentExecutor.execute()` in a `synchronized` block or `ReentrantLock`.

```java
public synchronized void execute(String sql, StreamObserver<RowBatch> observer) { ... }
```

**Tradeoffs:**


| Aspect          | Benefit                                   | Cost                                                  |
| --------------- | ----------------------------------------- | ----------------------------------------------------- |
| Simplicity      | One-line fix                              | Zero concurrency — fragments queue behind each other  |
| Latency         | Predictable (no contention inside DuckDB) | P99 degrades linearly with fragment count per worker  |
| Future-proofing | None                                      | Must be ripped out for parallel execution in Phase 3+ |


**Verdict:** Acceptable only as a stop-gap before implementing Option A.

#### Option C: One Connection Per Request (No Pool)

Create a fresh `Connection` in every `executeFragment` call, execute, close.

**Tradeoffs:**


| Aspect          | Benefit                                  | Cost                                                                                             |
| --------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Thread safety   | Each thread owns its connection          | Connection creation cost (~2–5 ms for DuckDB) on every RPC                                       |
| Data visibility | Each new connection sees committed state | Must ensure data was loaded on a **shared database** (file-backed), not per-connection in-memory |
| Simplicity      | No pool management                       | Incompatible with `:memory:` databases — each connection gets its own empty DB                   |


**Critical limitation:** With in-memory DuckDB (`jdbc:duckdb:`), each `DriverManager.getConnection("jdbc:duckdb:")` creates a **separate, empty database**. To share data across connections you must either:

- Use a file-backed DB (`jdbc:duckdb:/path/to/worker.db`), or
- Use named in-memory databases (DuckDB supports this but JDBC support is inconsistent).

This effectively forces the architecture toward file-backed storage anyway — which ties into Problem 2.

### Recommendation

**Option A (connection pool) with file-backed DuckDB.** This:

- Eliminates the concurrency bug.
- Enables parallel fragment execution (Phase 3 prerequisite).
- Pairs naturally with the data locality solution below (workers store data in local `.duckdb` files).
- Pool size of 2–4 connections is sufficient given DuckDB's internal parallelism.

---



## Problem 2: Data Locality



### Current State

Workers generate synthetic data in-memory at startup via `WorkerDemoDataLoader.initialize(conn, shardIndex, shardCount)`. This inserts 3–4 hardcoded rows per shard into an in-memory database.

This has three problems:

1. **No real data** — only works for demos.
2. **No persistence** — restarting a worker loses all data.
3. **No locality awareness** — the coordinator has no knowledge of what data lives where; it just assumes `shardId % workerCount` routing.



### How Apache Impala Achieves Data Locality on HDFS

Impala's model (relevant to DuckCluster's design):

```
┌──────────────────────────────────────────────────────────────────┐
│  Impala Architecture (simplified)                                 │
│                                                                   │
│  Catalog Service                                                  │
│    - Knows table → partition → HDFS block mapping                 │
│    - Knows which DataNodes hold each block (via NameNode)         │
│                                                                   │
│  Coordinator (Impala Daemon)                                      │
│    - Receives query                                               │
│    - Asks catalog: "which nodes have blocks for partition X?"     │
│    - Schedules scan fragments on the node that HAS the data       │
│    - Falls back to remote read if local node is overloaded        │
│                                                                   │
│  Worker (Impala Daemon, co-located with DataNode)                 │
│    - Reads HDFS blocks via short-circuit local read (zero-copy)   │
│    - Processes scan locally — no network transfer for input data  │
│                                                                   │
│  Key principle: MOVE COMPUTATION TO DATA, not data to compute     │
└──────────────────────────────────────────────────────────────────┘
```

Key Impala concepts:

1. **Block location metadata** — The catalog knows physical data placement.
2. **Locality-aware scheduling** — Fragments are assigned to the node that already stores the relevant data blocks.
3. **Short-circuit reads** — Workers read local HDFS blocks without going through the DataNode RPC (DFS client reads the file directly).
4. **Graceful degradation** — If the local node is overloaded, the scheduler sends work to a remote node that reads over the network (slower but prevents hotspots).



### Proposed Solutions for DuckCluster



#### Option A: File-Per-Shard with Coordinator Metadata Registry (Recommended)

Each worker stores its assigned shards as **local DuckDB database files**. The coordinator maintains a metadata registry mapping `(table, shard) → worker`.

```
┌─────────────────────────────────────────────────────────────────────┐
│  Coordinator                                                         │
│                                                                       │
│  ShardCatalog (in-memory, built at startup or via admin API)         │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  table: "events"                                               │  │
│  │  shards:                                                       │  │
│  │    shard-0 → worker-1 (primary), worker-2 (replica, optional)  │  │
│  │    shard-1 → worker-2 (primary)                                │  │
│  │    shard-2 → worker-3 (primary)                                │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  Scheduler uses ShardCatalog to route fragments to the node that     │
│  OWNS the shard — no predicate-based partitioning needed at query    │
│  time if data is pre-partitioned.                                    │
└─────────────────────────────────────────────────────────────────────┘

Workers:
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  worker-1   │    │  worker-2   │    │  worker-3   │
│             │    │             │    │             │
│  /data/     │    │  /data/     │    │  /data/     │
│  events_0.db│    │  events_1.db│    │  events_2.db│
│             │    │             │    │             │
│  DuckDB     │    │  DuckDB     │    │  DuckDB     │
│  ATTACH     │    │  ATTACH     │    │  ATTACH     │
│  events_0   │    │  events_1   │    │  events_2   │
└─────────────┘    └─────────────┘    └─────────────┘
```

**Data loading workflow:**

1. **Offline ingestion (CSV/Parquet split):**
  ```bash
   # Pre-split data by shard key, place on each worker's local filesystem
   # Example: partition 1M-row events.parquet into 3 shards
   duckdb -c "COPY (SELECT * FROM 'events.parquet' WHERE hash(user_id) % 3 = 0)
              TO '/worker-1/data/events_shard0.parquet'"
   # ... repeat for each shard
  ```
2. **Worker startup:**
  ```java
   // Worker scans its local /data/ directory
   // ATTACHes each .duckdb file or imports each .parquet into its local DB
   for (Path shardFile : localDataDir.list()) {
       statement.execute("CREATE TABLE " + tableName +
           " AS SELECT * FROM read_parquet('" + shardFile + "')");
   }
  ```
3. **Worker registration (enhanced):**
  ```protobuf
   message RegisterWorkerRequest {
     string worker_id = 1;
     string host = 2;
     uint32 port = 3;
     repeated ShardOwnership owned_shards = 4;  // NEW
   }

   message ShardOwnership {
     string table_name = 1;
     uint32 shard_id = 2;
     uint64 row_count = 3;
   }
  ```
4. **Coordinator scheduling:**
  - When generating fragments, the coordinator looks up `ShardCatalog` to find which worker owns shard N.
  - Routes the fragment directly to that worker — **no partition predicate injection needed** since the worker only has its own shard's data.
  - If a worker is DOWN, the coordinator can either: (a) fail that shard, or (b) route to a replica if one exists.

**Fragment SQL simplification:**

With pre-partitioned data, fragments become simpler:

```sql
-- Before (predicate-based, worker has ALL data):
SELECT category, COUNT(*) FROM events WHERE (hash(user_id) % 3) = 0 GROUP BY category

-- After (locality-based, worker has ONLY its shard):
SELECT category, COUNT(*) FROM events GROUP BY category
```

No shard predicate needed because the worker's `events` table already contains only its partition.

**Tradeoffs:**


| Aspect            | Benefit                                                              | Cost                                                 |
| ----------------- | -------------------------------------------------------------------- | ---------------------------------------------------- |
| Query performance | No predicate filtering on every row; DuckDB scans only relevant data | Requires upfront data partitioning                   |
| Data loading      | Explicit, auditable (files on disk)                                  | Operational step before queries work                 |
| Persistence       | Workers survive restarts without re-loading                          | Disk usage (but DuckDB files are compact)            |
| Scheduling        | Deterministic shard→worker routing                                   | Less flexible — adding a worker requires resharding  |
| Fragment SQL      | Simpler (no injected WHERE clauses)                                  | Coordinator must trust the shard catalog is accurate |
| Failure handling  | Clear ownership model                                                | Lost worker = lost shard (unless replicated)         |




#### Option B: Coordinator-Pushed Data Distribution

The coordinator holds the source data (or access to it) and pushes partitioned subsets to workers via gRPC at table-registration time.

```
Admin registers table → Coordinator reads source → Splits → Pushes to workers via gRPC
```

**New gRPC surface:**

```protobuf
service WorkerService {
  rpc LoadData(stream DataBatch) returns (LoadDataResponse);  // NEW
  rpc ExecuteFragment(FragmentRequest) returns (stream RowBatch);
}
```

**Tradeoffs:**


| Aspect              | Benefit                                       | Cost                                                               |
| ------------------- | --------------------------------------------- | ------------------------------------------------------------------ |
| Simplicity for user | Single API call loads everything              | Coordinator becomes a bottleneck (reads + fans out entire dataset) |
| No pre-split needed | Coordinator handles partitioning              | Network transfer scales as O(data_size) through coordinator        |
| Dynamic resharding  | Coordinator can re-push when topology changes | Expensive — full table transfer                                    |
| Memory pressure     | Workers receive only their shard              | Coordinator must buffer or stream the full dataset                 |


This model works for small-to-medium datasets (< 1 GB) but does not scale to OLAP workloads. It's the opposite of Impala's "move compute to data" principle — it moves data to compute.

#### Option C: Shared Storage with Locality Hints (Impala-like, Stretch)

Workers and coordinator access a **shared filesystem** (NFS, S3, or HDFS). Data files are partitioned (e.g., Hive-style `table/shard=0/data.parquet`). Workers read only their assigned partitions.

```
Shared Storage (S3 / NFS / HDFS):
  /warehouse/events/shard=0/part-0000.parquet
  /warehouse/events/shard=1/part-0000.parquet
  /warehouse/events/shard=2/part-0000.parquet

Workers:
  worker-1 reads shard=0 directly from shared storage
  worker-2 reads shard=1 directly from shared storage
  worker-3 reads shard=2 directly from shared storage
```

**Tradeoffs:**


| Aspect                 | Benefit                                                           | Cost                                                     |
| ---------------------- | ----------------------------------------------------------------- | -------------------------------------------------------- |
| Elasticity             | Add workers without moving data                                   | Requires shared storage infrastructure                   |
| Data locality          | Only achievable with HDFS (co-located DataNodes) or local caching | S3/NFS = always remote reads                             |
| DuckDB integration     | `read_parquet('s3://...')` works natively                         | Network I/O on every query (no locality without caching) |
| Operational simplicity | Standard data lake patterns                                       | Significant infrastructure dependency                    |
| True Impala parity     | Block-level locality with HDFS                                    | Overkill for a v1 distributed query engine               |




### Recommendation

**Option A (file-per-shard with metadata registry)** for the following reasons:

1. **Closest to Impala's principle** — data is local to the worker that will process it. No network reads at query time.
2. **Natural fit with DuckDB** — file-backed databases are first-class in DuckDB; `ATTACH` is zero-overhead.
3. **Enables the connection pool** — file-backed DBs allow multiple connections to the same database.
4. **Operationally simple** — split Parquet/CSV files, place them, start workers. No shared storage needed.
5. **Correct for the project's scale** — DuckCluster is a learning/demo system; S3 integration (Option C) is a stretch goal.

---



## Combined Implementation Plan

The two solutions reinforce each other:

```
┌───────────────────────────────────────────────────────────────────────┐
│  Before                              After                             │
│                                                                        │
│  Worker starts                       Worker starts                     │
│    → opens in-memory DB                → opens file-backed DB          │
│    → inserts hardcoded rows              (or creates + imports parquet)│
│    → single Connection                 → creates connection pool (N=4) │
│    → fragments execute serially        → fragments execute in parallel │
│                                        → shard catalog tells coord     │
│                                          which worker owns what        │
└───────────────────────────────────────────────────────────────────────┘
```



### Suggested Ordering

1. **Switch to file-backed DuckDB** — change connection string from `"jdbc:duckdb:"` to `"jdbc:duckdb:/data/worker-{id}.db"`. Existing demo data loader writes to this file.
2. **Implement connection pool** — `DuckDBConnectionPool` with `ArrayBlockingQueue`, pool size = CPU count - 1 (min 2).
3. **Update** `FragmentExecutor` — checkout/checkin pattern instead of holding one connection.
4. **Add** `ShardOwnership` **to worker registration** — workers report which tables/shards they have.
5. **Update coordinator scheduler** — route by ownership, not `shardId % workerCount`.
6. **Replace demo loader with file-based ingestion** — scripts to split Parquet/CSV and place on workers.

---



## Decisions (Resolved)

1. **Pool exhaustion policy:** Hybrid — block for a short configurable duration (`DUCKCLUSTER_POOL_WAIT_MS`, default 200ms) to allow in-flight queries to finish and free a connection. If no connection becomes available within that window, return `RESOURCE_EXHAUSTED` to the coordinator, which then reschedules the fragment to a replica. If no replica is available, fall back to a coordinator-mediated remote read. This avoids unnecessary rescheduling for transient load spikes while still providing backpressure under sustained overload.
2. **Hot-adding shards + automatic replication:** A background watcher thread on each worker monitors its local data directory (`WatchService` / polling, 2-second interval configurable via `DUCKCLUSTER_WATCHER_INTERVAL_MS`). When a new `.duckdb` shard file appears, the worker ATTACHes it via a dedicated admin connection (outside the query pool) and notifies the coordinator's `ShardCatalog` via a gRPC call. The coordinator then checks the consistent hash ring: if the shard doesn't yet have enough replicas (fewer than `REPLICATION_FACTOR`), the coordinator orchestrates replication by streaming the shard from the reporting worker to the other workers that should own it (per the ring). If a query arrives for a shard that isn't yet in the catalog, it fails fast with a clear error ("shard not registered"). The race window (up to 2s) is acceptable.
3. **Replication:** Required. Controlled by a configurable replication factor (`DUCKCLUSTER_REPLICATION_FACTOR`, default 2). Each shard is stored on N workers. The coordinator's `ShardCatalog` tracks primary + replica owners. On pool exhaustion or worker failure, the coordinator routes to a replica. On total loss of all replicas, the query fails for that shard.
4. **Data format:** `.duckdb` files. Workers `ATTACH` them directly — zero import cost, instant availability. Trade-off: vendor lock-in to DuckDB format, but acceptable since the entire system is DuckDB-native.
5. **Shard-to-worker mapping:** Consistent hashing ring. Workers are placed on the ring by hash of their ID. Shards are assigned to the N closest workers on the ring (where N = replication factor). When a worker is added or removed, only ~`1/workerCount` of shards need to migrate — not a full reshuffle. This makes elastic scaling operationally viable.
6. **Online rebalancing (no downtime):** When workers join/leave, rebalancing is online — the old owner continues serving queries for a shard while the `.duckdb` file is being copied to the new owner. Ownership transfers only after: (a) the file copy completes, (b) the new worker's watcher ATTACHes it, and (c) the coordinator's `ShardCatalog` is updated. Until that moment, all queries route to the old owner as usual. No "maintenance window" or shard unavailability during migration. This works cleanly because the system is read-only — there's no divergence risk between old and new owners during the copy.
7. **Read-only system:** DuckCluster is a read-only analytics framework. No write path, no INSERT routing, no replica consistency protocol. Shards are immutable once placed. This eliminates an entire class of distributed systems complexity (consensus, conflict resolution, write-ahead logs).
8. **ATTACH naming:** Each shard DB is ATTACHed with a unique catalog name (`ATTACH '<path>' AS events_shard0`). Fragment SQL uses qualified names (`SELECT * FROM events_shard0.events`). The coordinator's fragment generator is responsible for qualifying table references based on the shard assignment.
9. **Remote read topology:** Coordinator-mediated in v1. The coordinator streams data from the shard owner and pushes to the executing worker. Simpler communication pattern; acceptable because remote reads are a last resort (replication factor ≥ 2 makes them rare).
10. **Admin connection:** A single dedicated connection outside the pool, used exclusively by the `ShardFileWatcher` for `ATTACH`/`DETACH` DDL. Prevents schema-change locks from blocking query-serving pool connections.
11. **Pool size:** Start with `min(4, availableProcessors - 1)` (minimum 2). Exposed as `DUCKCLUSTER_POOL_SIZE` for tuning. Will revisit after stress testing.

---



## Summary


| Problem                 | Solution                                                                      | Key Tradeoff                                                                        |
| ----------------------- | ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| Connection concurrency  | Bounded connection pool (2–4 conns per worker) over file-backed DuckDB        | Slightly higher memory per worker; required for correctness                         |
| Data locality           | Pre-partitioned local `.duckdb` files + shard catalog with replication        | Requires offline data prep                                                          |
| Pool exhaustion         | Brief block (200ms) → RESOURCE_EXHAUSTED → route to replica                   | Small latency bump on transient spikes; avoids unnecessary rescheduling             |
| Worker topology changes | Consistent hashing ring + online rebalance                                    | Old owner serves until new owner is ready; copy duration = migration time           |
| Shard discovery         | Background watcher thread (2s poll) + catalog notification + auto-replication | Coordinator becomes replication orchestrator; network cost during replica creation  |
| ATTACH naming           | Qualified catalog names (`ATTACH ... AS events_shard0`)                       | Fragment SQL must be catalog-qualified; coordinator fragment generator handles this |


Both changes are **prerequisites for Phase 3** (parallel fragment execution). Without the connection pool, parallel dispatch crashes workers. Without data locality, the system can't scale beyond hardcoded demo data.

---



## Remaining Drawbacks & Considerations

These are consequences of the chosen approach that don't have clean solutions — flagged so they don't become surprises during implementation.

1. **Online rebalance duration:** While the old owner continues serving during migration (no downtime), the migration is only complete once the file copy finishes and the new worker's watcher picks it up. For large shards (multi-GB `.duckdb` files), this copy can take minutes over the network. During this window the new worker is underutilized and the old owner carries extra load. Acceptable for DuckCluster's scale; a production system would want progress tracking and throttling.
2. **Coordinator as replication orchestrator:** When a hot-added shard needs replicas, the coordinator streams it from the source worker and pushes to replica targets. This puts the coordinator on the data path temporarily — it must buffer or stream-through the full shard. For large shards this adds coordinator memory pressure and network bandwidth usage. Mitigations: (a) stream-through without buffering (pipe `ReadShard` output directly into `LoadTempData` calls), (b) throttle concurrent replication to 1–2 shards at a time. If this becomes a bottleneck at scale, a future version could do worker-to-worker transfers.
3. **Remote read fallback is expensive:** When all local replicas are exhausted and the coordinator falls back to a remote read, it must stream the shard's relevant data over gRPC before execution. Significantly slower than local reads. The 200ms block-before-reschedule mitigates spurious fallbacks, but under sustained load this path will trigger. If it triggers frequently, the cluster is under-provisioned (add workers or increase replication factor).
4. **Qualified SQL adds fragment generator complexity:** Since each shard is ATTACHed as a named catalog (`events_shard0`), the Calcite-generated fragment SQL must produce qualified table references (`events_shard0.events` instead of just `events`). This means the fragment generator needs to know the exact catalog name for the target shard — tighter coupling between the coordinator's scheduling decision and its SQL generation step.
5. **Admin connection serializes schema changes:** Only one ATTACH/DETACH can run at a time (single admin connection). If many shard files land simultaneously (bulk ingestion or replication of multiple shards), they queue. For typical operations (adding a few shards) this is fine. For bulk bootstrap of 100+ shards, the worker startup time is dominated by sequential ATTACHes. Mitigation: at startup, batch-scan the data directory and ATTACH all existing files before starting the gRPC server (no watcher needed for the initial set).
6. **Consistent hashing with low worker count:** With few workers (3–5), the hash ring can be uneven — some workers get more shards than others. Mitigation: use virtual nodes (multiple ring positions per physical worker, e.g., 100 vnodes each). Standard practice, but adds implementation complexity.
7. **200ms block window is a latency floor for the overload path:** Under pool exhaustion, the fastest possible reschedule takes 200ms (the full block duration). For latency-sensitive queries hitting an overloaded worker, this adds a fixed penalty before the coordinator can try a replica. Tunable via `DUCKCLUSTER_POOL_WAIT_MS` — set lower (50ms) for latency-sensitive deployments, higher (500ms) for throughput-oriented ones.
8. **Replication during rebalance is coordinator-initiated, not self-healing:** If a worker dies permanently and its shards now have fewer replicas than the configured factor, the coordinator must actively re-replicate those shards to other workers. This requires the coordinator to detect "under-replicated shards" and trigger replication — adding a background reconciliation loop. Without it, the cluster degrades silently after failures (fewer replicas = less redundancy).

---



## Implementation TODOs



### Phase A: File-Backed DuckDB + Connection Pool ✅

- [x] **A1. Switch worker storage to file-backed DuckDB**
  - Change `WorkerDemoDataLoader.openInMemoryConnection()` to use `"jdbc:duckdb:<dataDir>/worker-<id>.db"`
  - Add `DUCKCLUSTER_DATA_DIR` env var (default: `./data`) to `ClusterConfig`
  - Ensure the data directory is created at startup if missing
  - Existing demo data loader writes to the file-backed DB (data persists across restarts)

- [x] **A2. Implement** `DuckDBConnectionPool`
  - New class in `worker/src/main/java/io/duckcluster/worker/duckdb/DuckDBConnectionPool.java`
  - Constructor: `(String dbPath, int poolSize)` — opens N connections to the same `.duckdb` file
  - `Connection checkout(long timeoutMs)` — blocks up to `DUCKCLUSTER_POOL_WAIT_MS` (default 200ms), returns null if timeout expires
  - `void checkin(Connection conn)` — return connection to pool
  - `boolean isExhausted()` — returns true when all connections are checked out
  - `void close()` — closes all connections on shutdown
  - Pool size from `DUCKCLUSTER_POOL_SIZE` env var (default: `min(4, availableProcessors - 1)`, minimum 2)
  - Connection health check on checkout: `conn.isValid(1)` — if invalid, replace with a fresh connection
  - Add `DUCKCLUSTER_POOL_WAIT_MS` env var (default: 200) to `ClusterConfig`

- [x] **A3. Dedicated admin connection** *(deferred to Phase C — not needed until ShardFileWatcher)*
  - One extra `Connection` outside the pool, created at startup
  - Used exclusively by `ShardFileWatcher` (Phase C) for `ATTACH`/`DETACH` DDL
  - Prevents schema-change catalog locks from blocking pool connections
  - At startup, batch-ATTACH all existing shard files via this connection before starting gRPC server

- [x] **A4. Update** `FragmentExecutor` **to use pool**
  - Replace `private final Connection connection` with `private final DuckDBConnectionPool pool`
  - `execute(sql, observer)`: checkout (with 200ms timeout) → try/finally checkin
  - On checkout returning null (timeout): call `observer.onError(Status.RESOURCE_EXHAUSTED.withDescription("pool exhausted"))`
  - The gRPC error code signals the coordinator to try a replica

- [x] **A5. Update** `WorkerMain` **wiring**
  - Create `DuckDBConnectionPool` + admin connection instead of a single `Connection`
  - Pass pool to `FragmentExecutor`
  - Shutdown hook closes pool and admin connection

- [x] **A6. Unit test: concurrent fragment execution**
  - Spawn N threads (where N > pool size), each calling `fragmentExecutor.execute(...)` simultaneously
  - Verify: threads up to pool size complete successfully; excess threads receive `RESOURCE_EXHAUSTED` after 200ms
  - Verify pool checkout/checkin lifecycle (no leaks after all calls complete)
  - Verify connection health check replaces broken connections



### Phase B: Consistent Hashing + Shard Catalog ✅

- [x] **B1. Implement** `ConsistentHashRing`
  - New class: `common/src/main/java/io/duckcluster/common/routing/ConsistentHashRing.java`
  - Hash function: SHA-256 truncated to 32 bits
  - Virtual nodes: 100 per physical worker (configurable via `DUCKCLUSTER_VNODES_PER_WORKER`)
  - Methods: `addWorker(workerId)`, `removeWorker(workerId)`, `getOwners(shardKey, replicaCount) → List<WorkerId>` (returns N closest workers on ring)
  - Thread-safe (ReentrantReadWriteLock — reads dominate)
  - Deterministic: same set of workers + same shard key → same assignment everywhere

- [x] **B2. Extend protobuf:** `ShardOwnership` **in registration**
  - Added `repeated ShardOwnership owned_shards` to `RegisterWorkerRequest`
  - Defined `ShardOwnership { string table_name; uint32 shard_id; uint64 row_count; }`
  - Added new RPC: `rpc UpdateShardOwnership(UpdateShardRequest) returns (UpdateShardResponse)` — handler stubbed for Phase C

- [x] **B3. Implement** `ShardCatalog` **on coordinator**
  - New class: `coordinator/src/main/java/io/duckcluster/coordinator/catalog/ShardCatalog.java`
  - Thin wrapper over `ConsistentHashRing` — ring determines assignment; Phase C adds actual ownership tracking
  - Thread-safe via ring's internal locking
  - Methods: `onWorkerAdded(workerId)`, `onWorkerRemoved(workerId)`, `getOwners(tableName, shardId) → List<WorkerId>`

- [x] **B4. Update coordinator scheduler to use** `ShardCatalog`
  - Replaced `fragment.shardId() % workers.size()` with `shardCatalog.getOwners(table, shardId)`
  - `executeWithFallback()`: tries primary → replicas; on `RESOURCE_EXHAUSTED` tries next owner
  - Logs scheduling outcome: `LOCAL` / `REPLICA`

- [x] **B5. Handle** `RESOURCE_EXHAUSTED` **from workers**
  - When coordinator receives `RESOURCE_EXHAUSTED` gRPC status: retry the fragment on the next replica in the ownership list
  - If all replicas exhausted: throws `IllegalStateException` with clear message (remote read fallback in Phase E)

- [x] **B6. Wire replication factor + consistent hashing configs**
  - Added to `ClusterConfig`:
    - `DUCKCLUSTER_REPLICATION_FACTOR` (default: 2)
    - `DUCKCLUSTER_VNODES_PER_WORKER` (default: 100)
  - `CoordinatorMain` creates ring + catalog and passes to `QueryExecutionService`
  - `CoordinatorGrpcServer` calls `shardCatalog.onWorkerAdded()` on worker registration

- [x] **B7. Update fragment SQL generation for qualified table names**
  - Calcite fragment generator produces `SELECT ... FROM <catalog_name>.<table_name>` instead of just `<table_name>`
  - Catalog name derived from shard assignment: e.g., shard 3 of table "events" → `events_shard3.events`
  - `ShardPredicateInjector` removed — shard predicates no longer needed with file-per-shard model
  - Coordinator passes the catalog name to the fragment generator alongside the shard ID



### Phase C: Shard File Watcher + Dynamic Discovery ✅

- [x] **C1. Implement** `ShardFileWatcher` **on worker**
  - New class: `worker/src/main/java/io/duckcluster/worker/duckdb/ShardFileWatcher.java`
  - Background daemon thread using `ScheduledExecutorService` polling at configurable interval (`DUCKCLUSTER_WATCHER_INTERVAL_MS`, default 2000ms)
  - Watches `DUCKCLUSTER_DATA_DIR` for new/deleted `*.duckdb` files matching naming convention
  - On new file: ATTACH via `ShardManager`, notify coordinator via `UpdateShardOwnership` gRPC
  - On file deleted: DETACH via `ShardManager`, notify coordinator

- [x] **C2. Define shard file naming convention**
  - Convention: `<table>_shard<id>.duckdb` (e.g., `events_shard0.duckdb`, `orders_shard5.duckdb`)
  - Parsed by `ShardFileMetadata.fromPath()` using regex: `^([a-zA-Z_][a-zA-Z0-9_]*)_shard(\d+)\.duckdb$`
  - After ATTACH: accessible as `events_shard0.events` (catalog.table)

- [x] **C3. Startup batch-ATTACH**
  - `ShardManager.scanAndAttachAll()` scans data dir for all `*.duckdb` files before registration
  - ATTACHes via dedicated admin connection (outside pool)
  - Reports full ownership list in `RegisterWorkerRequest.owned_shards`
  - `CoordinatorGrpcServer` processes owned_shards and calls `shardCatalog.registerOwnership()`

- [x] **C4. Coordinator: handle** `UpdateShardOwnership` **RPC + trigger replication**
  - Fleshed out handler in `CoordinatorGrpcServer` — updates `ShardCatalog.registerOwnership()`
  - `ShardCatalog` now tracks actual ownership (`Map<workerId, Set<shardKey>>`)
  - `getActualOwners(table, shardId)` and `getUnderReplicatedShards()` methods added
  - Under-replication detected by comparing actual owners vs replication factor

- [x] **C5. Coordinator-orchestrated shard replication**
  - `ShardReplicator` background reconciliation loop (30s interval)
  - Calls `shardCatalog.getUnderReplicatedShards()`, picks source (has it) and targets (should have it)
  - Streams: `WorkerNodeClient.streamShardFrom()` → `pushShardTo()` (raw file bytes, 64KB chunks)
  - Max 2 concurrent replications (Semaphore)
  - Idempotent: skips if target already reports ownership
  - Idempotent: if target already has the shard, skip

- [x] **C6.** `ReadShard` + `ReceiveShard` **RPCs on worker**
  - `ReadShard`: server-streaming RPC — reads shard `.duckdb` file and streams as 64KB `ShardDataChunk` messages
  - `ReceiveShard`: client-streaming RPC — receives chunks, writes to `.tmp` file, atomic rename to `.duckdb` on `is_last`
  - Watcher picks up the new file on next poll cycle → ATTACHes → reports to coordinator

- [x] **C7. Coordinator replication reconciliation loop**
  - Implemented in `ShardReplicator` — `ScheduledExecutorService` every 30s
  - Calls `shardCatalog.getUnderReplicatedShards()` which checks actual vs ring-expected
  - Queues replication tasks for under-replicated shards
  - Handles worker death: removed worker's shards lose a replica → reconciliation re-replicates



### Phase D: Data Ingestion Tooling

- [x] **D1. Write** `split-and-distribute.sh` **script**
  - Input: source data file (Parquet/CSV), table name, shard key column, shard count, worker data directories
  - Uses `ConsistentHashRing` logic (or a standalone Java CLI that prints the ring assignment) to determine which shards go to which workers (respecting replication factor)
  - Splits source data into N `.duckdb` shard files using DuckDB CLI:
    ```bash
    duckdb -c "ATTACH '<worker-dir>/<table>_shard<i>.duckdb' AS out;
               CREATE TABLE out.<table> AS
               SELECT * FROM '<source>' WHERE hash(<key>) % <N> = <i>;"
    ```
  - Places each shard file on `replication_factor` worker directories per the ring assignment
  - Outputs a manifest JSON: `{ "events_shard0": ["worker-1", "worker-3"], ... }`

- [x] **D2. Online rebalance via coordinator**
  - Triggered when a worker is added/removed (coordinator detects via registration or heartbeat loss)
  - Coordinator recomputes the consistent hash ring with the new topology
  - Identifies shards that should now live on different workers (~1/N of total shards)
  - **Online migration flow:**
    1. Old owner continues serving queries (no ownership change yet)
    2. Coordinator streams shard from old owner → pushes to new owner via `ReceiveShard` (C6)
    3. New owner's watcher ATTACHes the file and reports ownership to coordinator
    4. Coordinator updates `ShardCatalog` — new owner is now primary
    5. Old owner's copy can be removed (optional cleanup) or kept as an extra replica
  - No downtime: queries always have at least one serving owner throughout the migration
  - Progress visible via SSE events: "migrating shard X from worker-A to worker-B (45% complete)"

- [x] **D3. Update** `start-cluster.sh` **to use file-based data**
  - Replace hardcoded `WorkerDemoDataLoader` inserts with a call to `split-and-distribute.sh` for demo data
  - Each worker starts, batch-ATTACHes its shard files, registers ownership with coordinator

- [x] **D4. Deprecate** `WorkerDemoDataLoader` **hardcoded inserts**
  - Keep as a fallback for unit tests only (creates a small in-memory DB for test isolation)
  - Remove from the production startup path in `WorkerMain`



### Phase E: Remote Read Fallback with Shard Caching ✅

- [x] **E1. Fix** `FragmentExecutor` **pool integration**
  - Refactored from single `Connection` to pool-based `checkout()`/`checkin()` per query execution
  - On pool exhaustion: returns `RESOURCE_EXHAUSTED` gRPC status
  - On missing catalog (cache eviction): returns `FAILED_PRECONDITION` gRPC status

- [x] **E2. Add** `LoadTempData` **RPC to** `WorkerService`
  - Client-streaming RPC: receives `ShardDataChunk` stream, writes to cache dir, ATTACHes shard
  - Returns `LoadTempDataResponse { accepted, catalog_name }`
  - Differs from `ReceiveShard`: writes to `<dataDir>/.cache/` (not dataDir), doesn't report ownership

- [x] **E3. Add** `UpdateShardCache` **RPC to** `CoordinatorService`
  - Workers push their cached shard state (full-replace semantics, same pattern as `UpdateShardOwnership`)
  - Coordinator tracks which workers have which shards cached for routing decisions

- [x] **E4. Implement** `TempShardCache` **on worker**
  - LRU cache with configurable max size (`DUCKCLUSTER_CACHE_MAX_SHARDS`, default 5)
  - Files stored in `<dataDir>/.cache/` — invisible to `ShardFileWatcher` (no false ownership reports)
  - Eviction: DETACH + delete file + notify coordinator via `UpdateShardCache`
  - `ShardManager` extended with `attachCachedShard()`/`detachCachedShard()` (separate map from owned shards)

- [x] **E5. 3-tier query routing in** `QueryExecutionService`
  - Tier 1: try ring owners (primary, replicas) — handles `RESOURCE_EXHAUSTED`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`
  - Tier 2: try workers with cached copy — handles `FAILED_PRECONDITION` (cache miss)
  - Tier 3: remote read fallback — stream shard from an owner to an idle worker via `LoadTempData`, then execute
  - All tiers exhausted → HTTP 503 with clear error message to client

- [x] **E6. Coordinator cache tracking**
  - `ShardCatalog` tracks cached shards per worker (`shardCache` map)
  - `getCachedWorkers(table, shardId)` returns workers with cache hit
  - Worker removal clears cache entries
  - Stale entries handled via lazy invalidation (FAILED_PRECONDITION → fall through)



### Phase F: Testing & Validation

- [ ] **F1. Unit test: ConsistentHashRing**
  - Add 5 workers, assign 100 shards — verify roughly even distribution (no worker gets >30% of shards)
  - Remove 1 worker — verify only ~20% of shards changed owner
  - Add 1 worker — verify only ~17% of shards migrated
  - Verify determinism: same inputs → same outputs across runs

- [ ] **F2. Integration test: pool exhaustion → replica routing**
  - Start 3 workers with replication factor 2
  - Send enough concurrent queries to exhaust worker-1's pool (more than pool size)
  - Verify: first N fragments succeed on worker-1, subsequent fragments get `RESOURCE_EXHAUSTED` after 200ms, coordinator routes to replica, returns correct result
  - Verify query stats show `REPLICA` scheduling outcome

- [ ] **F3. Integration test: worker failure → replica takeover**
  - Start 3 workers with replication factor 2
  - Kill worker-1 mid-query
  - Verify coordinator detects failure (heartbeat miss), routes subsequent queries to replica
  - Verify consistent hash ring update → new ring assignment logged

- [ ] **F4. Integration test: hot shard addition + auto-replication**
  - Start 3 workers with replication factor 2
  - Place a new `.duckdb` shard file on worker-1 only
  - Verify: watcher detects it within 2s, ATTACH succeeds, coordinator catalog updated
  - Verify: coordinator detects under-replication (1 < RF=2), initiates replication to worker-2 (per hash ring)
  - Verify: worker-2 receives the shard, ATTACHes it, reports ownership
  - Query the shard — verify correct results from either worker

- [ ] **F5. Integration test: qualified table name resolution**
  - Worker holds `events_shard0.duckdb` and `events_shard1.duckdb` (2 shards of same table)
  - Dispatch fragment with SQL `SELECT * FROM events_shard0.events`
  - Verify it returns only shard 0's data, not shard 1's

- [ ] **F6. Correctness test: concurrent queries produce correct results**
  - 10 concurrent GROUP BY queries across 3 workers, 6 shards, replication factor 2
  - Compare distributed results vs single-node DuckDB on the full dataset
  - Must match exactly (SUM, COUNT, MIN, MAX)

- [ ] **F7. Stress test: pool sizing + block duration validation**
  - Vary pool size (1, 2, 4, 8) and block duration (50ms, 200ms, 500ms) under fixed concurrency (16 queries)
  - Measure: throughput, p50/p99 latency, RESOURCE_EXHAUSTED rate, replica-routing rate
  - Establish recommended defaults

- [ ] **F8. Integration test: online rebalance (worker addition)**
  - Start 3 workers, load 9 shards (3 per worker, RF=2)
  - Add worker-4 to the cluster
  - Verify: coordinator computes new ring, identifies ~3 shards to migrate
  - Verify: during migration, queries continue succeeding (old owners serve)
  - Verify: after migration completes, new worker serves its assigned shards
  - Verify: no query failures or incorrect results throughout the process

- [ ] **F9. Integration test: reconciliation after worker death**
  - Start 3 workers, RF=2, 6 shards
  - Kill worker-1 permanently (don't restart)
  - Verify: coordinator marks worker-1 DEAD, identifies under-replicated shards
  - Verify: reconciliation loop re-replicates those shards to remaining healthy workers
  - Verify: all shards return to full replication factor

---



## No Remaining Open Questions

All architectural decisions have been resolved. Implementation can proceed phase-by-phase starting from Phase A.