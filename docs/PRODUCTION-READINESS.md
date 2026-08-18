# DuckCluster — Production Readiness Review

This document lists what to change to take DuckCluster from a working distributed prototype to something
you can run as a shared service. It is based on reading the coordinator, worker, planner, proto, CLI,
Docker, and CI code paths. Every item names the code it refers to so it can be verified and turned into an
issue.

Query-performance work that is already measured lives in [BENCHMARK.md](BENCHMARK.md) and
[OPTIMIZATIONS.md](OPTIMIZATIONS.md). Section 4 summarizes those levers rather than repeating the numbers.

**Priority** describes severity: **P0** = wrong results, hangs, or an exposure that matters as soon as
the cluster is shared. **P1** = needed to operate it as a service. **P2** = scale, polish, or platform work.
**Effort:** **S** = days, **M** = a week or two, **L** = multi-week / design work.

**Ordering:** functionality comes first. Sections 1–9 cover correctness, reliability, performance, data,
observability, APIs, operations, and testing. Security is grouped in section 10 and scheduled as the last
phase of the [order of work](#suggested-order-of-work), so its P0 labels reflect severity, not sequence.

---

## At a glance

| # | Item | Area | Priority |
|---|------|------|----------|
| 1.1 | Concurrent JOINs on one worker clobber each other's broadcast tables | Correctness | P0 |
| 1.2 | Shard pin sessions are worker-global and leak on cleanup failure | Correctness | P0 |
| 1.3 | NULLs and column types are lost on the wire | Correctness | P0 |
| 1.4 | Workers never re-register after a coordinator restart | Reliability | P0 |
| 2.1 | No deadlines on any RPC | Reliability | P0 |
| 3.1 | Unbounded thread pools, no admission control | Resources | P0 |
| 2.4 | Coordinator is a single point of failure; metadata is not persisted | Reliability | P1 |
| 10.1 | No authentication on the HTTP API | Security | P0 |
| 10.2 | Worker gRPC runs arbitrary SQL from any caller, in plaintext | Security | P0 |
| 10.4 | Path traversal in shard transfer RPCs | Security | P0 |

---

## 1. Correctness bugs (fix first)

### 1.1 Concurrent JOINs clobber each other's broadcast tables · P0 · S–M

**Where:** [`BroadcastSqlNames.tempTable`](../common/src/main/java/io/duckcluster/common/planner/BroadcastSqlNames.java)
names the materialized table `__dc_bcast_<table>`, with no query id.
[`BroadcastMaterializer.materialize`](../worker/src/main/java/io/duckcluster/worker/duckdb/BroadcastMaterializer.java)
runs `DROP TABLE IF EXISTS` then `CREATE TABLE` on that shared name. The worker's `clearBroadcastTables`
handler ignores `query_id` and calls `clearAll()`.

**Problem:** When two queries on the same worker join the same multi-shard broadcast table, query B's
materialize drops the table query A is reading, and A's cleanup drops B's table. Under concurrency that
means errors or wrong results. The existing concurrency test runs a single-table `GROUP BY`, so it never
exercises this path.

**Fix:** Use query-scoped names (`__dc_bcast_<queryId>_<table>`) passed through `FragmentSqlGenerator`,
track materialized tables per query, and drop only that query's tables. A later option is a
reference-counted shared materialization keyed by table version (see 4.3).

### 1.2 Shard pin sessions are worker-global and leak · P0 · S

**Where:** `BeginShardPinRequest` / `EndShardPinRequest` in [`cluster.proto`](../proto/src/main/proto/cluster.proto)
carry no fields. [`TempShardCache`](../worker/src/main/java/io/duckcluster/worker/duckdb/TempShardCache.java)
keeps one `pinSessionDepth` counter and pins every cached entry when a session begins.
`QueryExecutionService.releasePreparedWorkers` calls `clearBroadcastTables` and then `endShardPin` for
each worker in one loop, with no per-worker `try`.

**Problem:** If cleanup throws for one worker, or the coordinator crashes mid-query, the remaining workers
never get `EndShardPin`. Their depth never returns to zero, the cache stops trimming, and disk use grows
without bound. Pins also cover the whole cache rather than the shards a query actually uses.

**Fix:** Pin by query id with a lease timeout, release each worker in its own `try/finally`, and add a
worker-side reaper that expires abandoned pins.

### 1.3 NULLs and column types are lost on the wire · P0 · M

**Where:** [`FragmentExecutor`](../worker/src/main/java/io/duckcluster/worker/duckdb/FragmentExecutor.java)
reads every cell with `getString` and writes NULL as `""`. `Column.values` in the proto is
`repeated string`. On the coordinator, [`DuckDbMergeSupport`](../coordinator/src/main/java/io/duckcluster/coordinator/merger/DuckDbMergeSupport.java)
infers merge column types from the values themselves (regex `-?\d+` means BIGINT, and so on), and
[`DuckDbBulkInserter.normalizeValue`](../coordinator/src/main/java/io/duckcluster/coordinator/merger/DuckDbBulkInserter.java)
turns `""` back into NULL only for BIGINT and DOUBLE columns.

**Problem:**
- A NULL string comes back to the client as `''`, and NULL and `''` collapse into one group in a merged `GROUP BY`.
- String columns that look numeric (zip codes like `02134`) get typed as BIGINT and lose their leading zeros.
- Integers come back as floats. `SELECT COUNT(*) FROM events` returns `10.0` through the Docker stack.

**Fix:** Carry the column types from the worker's `ResultSetMetaData` in each `RowBatch`, plus a null
marker per value. The full fix is a typed columnar format (4.1). A short-term step is a `type` field per
column and a `repeated bool nulls`.

### 1.4 Workers never re-register after a coordinator restart · P0 · S

**Where:** [`CoordinatorClient.startHeartbeat`](../worker/src/main/java/io/duckcluster/worker/client/CoordinatorClient.java):
the heartbeat stream's `onError` only logs, the scheduled task keeps writing to the dead stream, and
registration happens once in `WorkerMain`. The heartbeat also always sends `load = 0.0`.

**Problem:** The coordinator rebuilds its registry and shard catalog only from worker registrations.
After a coordinator restart, it has zero workers until every worker is restarted by hand. This is the
biggest hole in the "rebuild state from workers" design.

**Fix:** When the stream errors or completes, reconnect with backoff and re-register with the current
shard and cache lists. Have the coordinator return a boot epoch so workers can tell a restart from a
blip. Send real load (in-flight fragments or pool utilization).

### 1.5 DuckDB version skew between shard writers and readers · P1 · S

**Where:** `duckdb.version` is `1.1.3` in [`pom.xml`](../pom.xml) (JDBC on workers and coordinator).
[`tests/integration/requirements.txt`](../tests/integration/requirements.txt) has `duckdb>=1.0.0` (CI
installed 1.5.5), and the [`Dockerfile`](../Dockerfile) runs `pip install duckdb` unpinned. The Python
package is what writes shard files.

**Problem:** Shard files written by a newer Python DuckDB are attached by an older JDBC driver. It works
today, but a storage-format change will break attaches in CI or Docker without any code change.

**Fix:** Pin one DuckDB version in all three places, upgrade the JDBC driver, and check the shard file's
storage version on ATTACH with a clear error.

### 1.6 Async result store accounting race · P2 · S

**Where:** [`AsyncQueryService`](../coordinator/src/main/java/io/duckcluster/coordinator/execution/AsyncQueryService.java)
updates `totalRetainedBytes` (a `volatile long`) with `+=` from several async threads. Expiry runs only
when an API call arrives. The service has no unit tests.

**Fix:** Use an `AtomicLong` (or synchronize eviction), add a scheduled sweeper, and add tests for TTL
expiry, capacity eviction, and the 410 responses.

---

## 2. Reliability and fault tolerance

### 2.1 No deadlines on any RPC · P0 · S

None of the blocking stubs in [`WorkerNodeClient`](../coordinator/src/main/java/io/duckcluster/coordinator/worker/WorkerNodeClient.java)
set `withDeadlineAfter`, so a hung worker holds a coordinator thread forever. `loadTempData` and
`pushShardTo` ignore the return value of `latch.await(60s)` and never cancel the stream on timeout.

**Fix:** Per-RPC deadlines from config, an overall query deadline propagated to fragments, and stream
cancellation on timeout.

### 2.2 No query cancellation · P1 · M

There is no `DELETE /v1/query/{id}`. On the worker, `FragmentExecutor` doesn't watch for gRPC call
cancellation and never calls `Statement.cancel()`, so work continues after a client gives up. Add a cancel
token per query that cancels the in-flight gRPC calls, has workers cancel the running statement, and
releases pins and broadcast tables. In the CLI, map Ctrl+C to cancel.

### 2.3 Membership flapping triggers data movement · P1 · M

[`HeartbeatMonitor`](../coordinator/src/main/java/io/duckcluster/coordinator/health/HeartbeatMonitor.java)
removes a worker from the hash ring after `interval × threshold` (15 s by default), and replication starts
right away. A GC pause or network blip moves ownership and copies shards, then moves them back.

**Fix:** Add a `SUSPECT` state that stops scheduling work on the worker but keeps it in the ring. Declare
it `DEAD` only after a longer grace period, delay replication, and add an explicit drain/decommission
operation for planned removals.

### 2.4 Coordinator is a single point of failure; metadata is not persisted · P1 · M→L

The worker registry, `ShardCatalog` ownership and cache maps, `ClusterCatalog`, async jobs, and query
activity all live in coordinator memory. Steps in order of effort:

1. **Fix 1.4** so the coordinator can rebuild state after a restart.
2. **Snapshot metadata to disk.** Periodically write catalog, ownership, and replication intent to a JSON
   file (atomic write-and-rename), load it on startup, then reconcile against worker reports. This makes
   restarts fast and keeps decisions that can't be rediscovered from shard files (per-table settings,
   tables not yet placed).
3. **High availability.** Active/standby coordinators with leader election (etcd, ZooKeeper, or Raft), or
   stateless coordinators backed by a shared metadata store.

`scripts/capture-media-snapshot.py` saves dashboard screenshots and monitor JSON for documentation. It is
not runtime metadata persistence and doesn't address this item.

### 2.5 Retry policy for preparation steps · P2 · S

Fragment execution retries on retryable gRPC statuses, but a failed shard prefetch or broadcast
materialization throws `IllegalStateException` and fails the whole query. Add bounded retries with backoff
and fall back to another source worker.

### 2.6 Graceful shutdown · P2 · S

Shutdown hooks stop the servers without draining in-flight queries. Add a drain mode (stop accepting,
finish what's running, then exit) on both coordinator and workers.

---

## 3. Resource management and admission control

### 3.1 Unbounded thread pools, no admission control · P0 · S–M

`QueryExecutionService.fragmentExecutor` and `AsyncQueryService.executor` are `newCachedThreadPool`, and
synchronous `POST /v1/query` holds a Jetty thread for the whole query. A burst of queries creates threads
without limit until the merge pool (4 connections, 30 s wait) starts timing out.

**Fix:** A bounded query queue with a configurable maximum number of running queries, rejecting the
overflow with `429`/`503` and `Retry-After`. Bound the fragment pool and cap fragment concurrency per query.

### 3.2 Coordinator merge has no memory ceiling · P1 · S

[`CoordinatorDuckDbPool`](../coordinator/src/main/java/io/duckcluster/coordinator/merger/CoordinatorDuckDbPool.java)
opens in-memory `jdbc:duckdb:` connections without `memory_limit` or `temp_directory`, and the pool size is
hardcoded to 4. Fragment results are also fully buffered in Java lists before the merge starts (4.2).
Set a memory limit and a spill directory, make the pool size configurable, and cap the result size on the
synchronous path.

### 3.3 Worker DuckDB limits · P1 · S

Worker connections set no `memory_limit` or `threads`. [BENCHMARK.md](BENCHMARK.md) records worker
crashes under sustained SF40+ load; explicit limits per worker are the first step.

### 3.4 gRPC message size · P2 · S

Channels use the default 4 MiB maximum inbound message size, and `FragmentExecutor` batches by row count
(1024). A batch of wide string rows can exceed the limit. Size batches by bytes and set
`maxInboundMessageSize` explicitly.

### 3.5 Disk management · P2 · S

`loadTempData` writes to `Files.createTempFile` in the system temp directory rather than the worker data
directory, so large prefetches can fill `/tmp`. Keep temp files under the data dir, add quotas for the
shard cache, and clean up stale `.tmp` files on startup.

---

## 4. Performance

Ordered by expected impact. Items 4.3 and 4.5 are already tracked in [BENCHMARK.md](BENCHMARK.md#remaining-bottlenecks);
the others come from the code paths above.

### 4.1 Typed columnar wire format (Arrow IPC) · L

Replace the all-string `Column` with Arrow IPC batches: DuckDB exports Arrow, and the coordinator's DuckDB
can scan Arrow directly. This fixes 1.3, removes string parsing and value-based type inference from the
merge, and shrinks payloads. Arrow Flight is the natural transport if the RPC layer is reworked.

### 4.2 Stream fragment results into the merge · M

`WorkerNodeClient.executeFragment` collects every batch into a `List`, and the merge starts only after all
fragments finish. Appending batches into the coordinator DuckDB as they arrive overlaps merge work with
slow fragments and caps Java heap use. For `CONCATENATE`, rows can stream straight to the client.

### 4.3 Broadcast join reform · L

This is the main remaining gap on Q03/Q07/Q08/Q09/Q18. Replace per-query materialization with a long-lived
per-worker dimension cache keyed by table version, and add co-partitioned or shuffle joins for
large-to-large joins. It also removes the race in 1.1.

### 4.4 Parallelize per-worker preparation · S

`prepareWorkersForQuery` prepares workers one at a time. For each worker it runs `BeginShardPin`, then
prefetches missing shards serially (`ensureShardsLocal`), then `MaterializeBroadcast`. Run workers in
parallel, and fold the three calls into a single `PrepareQuery` RPC to cut the fixed per-query RPC cost.

### 4.5 Parallel merge executor with backpressure · M

At 4 concurrent clients the suite is about 2.5× slower than at 1, because merges serialize on the
coordinator. A dedicated merge pool with backpressure (ties into 3.1) is the path to throughput wins.

### 4.6 Stream results to clients · M

The async API paginates a result that is already fully materialized in coordinator memory. A server-side
cursor fed directly by merge output would cut time-to-first-row and memory for large results.

### 4.7 Statistics for planning · L

The planner picks the driving table by shard count alone. `ShardOwnership.row_count` exists in the proto
but `WorkerMain` always sends `0`. Report real row counts and sizes, and use them to choose the driving
side and between broadcast and co-partitioned joins.

### 4.8 gRPC compression and channel tuning · S

Enable compression for shard transfers and row batches between hosts, and set keepalive options on the
long-lived channels.

---

## 5. Data management and catalog

### 5.1 The catalog has no schema · P1 · M

[`ClusterCatalog`](../common/src/main/java/io/duckcluster/common/model/ClusterCatalog.java) stores only a
table name and shard count, and records the partition key as `"rowid"` for tables discovered from
workers. Read column names and types from shard files on attach, expose them through
`GET /v1/catalog/tables` and `DESCRIBE`, validate queries before dispatch, and detect schema drift between
shard files of the same table.

### 5.2 Ingestion and table lifecycle · P1 · L

Data is loaded offline by scripts (`split-and-distribute.sh`, `prepare-demo-shards.py`), and workers find
new files by polling. Add create/drop table operations, append and replace through versioned shard sets
with an atomic version swap (so running queries see one consistent version), and a declared partition key.

### 5.3 Integrity checks on shard transfer · P1 · S

`ReadShard`, `ReceiveShard`, and `LoadTempData` send no size or checksum. Send both with the final chunk and
verify before moving the file into place or attaching it.

### 5.4 Replication controls · P2 · S

[`ShardReplicator`](../coordinator/src/main/java/io/duckcluster/coordinator/replication/ShardReplicator.java)
hardcodes 2 concurrent replications and a 30 s reconcile interval. Make both configurable, export the
under-replicated shard count as a metric, and alert when the replication factor can't be met.

### 5.5 Backup and restore · P2 · M

Document and automate snapshots of shard files to object storage, and the restore path.

---

## 6. Observability

### 6.1 Metrics endpoint · P1 · M

Add Prometheus `/metrics` (already a TODO in [PLAN-dashboard.md](PLAN-dashboard.md)): query latency
histograms by merge strategy, running and queued queries, fragment retries, `RESOURCE_EXHAUSTED` counts,
pool saturation, shard cache hits and misses, replication backlog, async store size, and JVM metrics.

### 6.2 Logging · P1 · S

Both `logback.xml` files use `FileAppender` with `append=false`, so every restart wipes the previous logs,
and there is no rotation. The pattern has no level, thread, or query id. Switch to
`RollingFileAppender` with append on, add `%level` and `%thread`, put the query id in the MDC, and pass it
to workers in gRPC metadata so one id ties together all logs for a query. Offer a JSON encoder option.

### 6.3 Distributed tracing · P2 · M

OpenTelemetry spans for HTTP request → plan → prefetch → fragment RPC → worker execution → merge.

### 6.4 Query profiles and EXPLAIN · P2 · M

`GET /v1/query/{id}/profile` with per-fragment worker, timings, rows, bytes, and retries (dashboard P5).
`EXPLAIN` returning fragment SQL and the merge plan (CLI TODO).

### 6.5 Liveness vs readiness · P2 · S

Split health into liveness and readiness. The coordinator is ready when at least one worker is registered
and the catalog is non-empty; a worker is ready after attaching shards and registering.

---

## 7. API and clients

### 7.1 Error model · P1 · S

The HTTP layer maps `IllegalArgumentException` to 400, every `IllegalStateException` to 503, and anything
else to 500. A client can't tell unsupported SQL from a dead worker. Define error codes such as
`UNSUPPORTED_SQL`, `PLANNING_ERROR`, `NO_WORKERS`, `TIMEOUT`, `CANCELLED`, and `RESOURCE_EXHAUSTED`, return
them in the JSON body, and use them for both the sync and async APIs.

### 7.2 API versioning and an OpenAPI spec · P2 · S

Publish an OpenAPI document for `/v1` and a policy for breaking changes.

### 7.3 Standard client protocols · P2 · L

Arrow Flight SQL or the PostgreSQL wire protocol would let JDBC/ODBC drivers and BI tools connect without
custom code. A Python DB-API client would cover notebooks.

### 7.4 CLI polish · P2 · S

Show phase and fragment progress while polling async queries, cancel on Ctrl+C (needs 2.2), shell
completion, and `--explain` (needs 6.4).

### 7.5 Document supported SQL · P2 · S

List what the planner supports and rejects. For example, `AVG` inside a computed expression is
unsupported (`NestedAggregateExtractor`), and `ORDER BY` must come before `LIMIT` in the shell.

---

## 8. Operations and deployment

### 8.1 Configuration · P2 · S

All settings come from environment variables with no validation, and several are hardcoded: merge pool
size, replication concurrency, reconcile interval, fragment batch size (1024 rows), and transfer chunk
size (64 KiB). Load from a config file with environment overrides, validate at startup, and log the
effective configuration.

### 8.2 Kubernetes manifests or a Helm chart · P2 · M

A StatefulSet with persistent volumes for workers, a Deployment for the coordinator, Services, probes
(6.5), and a PodDisruptionBudget.

### 8.3 Rolling upgrades · P2 · M

The proto has no protocol version. Add one to `RegisterWorker`, reject or handle mismatches, and document
the upgrade order (workers first or coordinator first).

### 8.4 Dependency hygiene · P2 · S

Upgrade the DuckDB JDBC driver (see 1.5) and Javalin (the startup log reports the version as over 600 days
old). Add Dependabot or Renovate and a dependency vulnerability scan in CI.

### 8.5 Split the Docker image · P2 · S

One image currently carries the JARs plus Python and DuckDB for shard preparation. Split it into a slim
runtime image and a separate datagen image.

---

## 9. Testing and CI

### 9.1 Tests that would have caught section 1 · P0 · S–M

- Concurrent JOIN queries routed to the same worker (1.1).
- Restart the coordinator while workers stay up, then run a query (1.4).
- Inject a failure during cleanup and assert pins are released (1.2).
- NULL vs `''`, leading-zero strings, and integer vs float result types (1.3).

### 9.2 Unit tests for the async query store · P1 · S

Cover TTL expiry, capacity and byte-budget eviction, and the 410 responses for `AsyncQueryService`.

### 9.3 CI improvements · P2 · S

Pin Python dependencies, add a `docker compose up` smoke test job, add static analysis (Error Prone or
SpotBugs) and a coverage report, and track flaky tests (one stress test needed a re-run in this review).

### 9.4 Performance regression gate · P2 · M

Run a small TPC-H scale factor in CI, store results as artifacts, and fail or warn on regressions beyond a
threshold.

---

## 10. Security

None of these matter for a laptop demo. All of them matter as soon as the cluster is on a shared network.
They are scheduled last, so until this phase lands, keep the coordinator and worker ports off untrusted
networks. `docker-compose.yml` already keeps worker ports internal, but it publishes coordinator HTTP and
gRPC, and `start-cluster.sh` binds everything on all interfaces.

### 10.1 No authentication on the HTTP API · P0 · M

`CoordinatorHttpServer` binds `0.0.0.0` by default (`DUCKCLUSTER_COORDINATOR_HTTP_BIND_HOST`), and anyone
who can reach it can run queries and read monitor data. Start with API tokens (bearer header), then add
users and roles. Default the bind to `127.0.0.1` outside Docker, and add rate limiting.

### 10.2 Worker gRPC runs arbitrary SQL from any caller · P0 · M

[`WorkerGrpcServer`](../worker/src/main/java/io/duckcluster/worker/grpc/WorkerGrpcServer.java) listens on
all interfaces (`ServerBuilder.forPort`), with no auth, and `ExecuteFragment` executes `request.getSql()`
as-is. Both directions use `usePlaintext()` (`WorkerChannelPool`, `CoordinatorClient`). Anyone who can
reach port 9101 can run any DuckDB SQL on a worker, including statements that read local files.

**Fix:** mTLS between coordinator and workers, bind worker ports to a private interface, and harden
worker DuckDB connections: `SET enable_external_access = false`, disable extension autoload/autoinstall,
then `SET lock_configuration = true`.

### 10.3 Worker registration can be spoofed · P1 · S

`RegisterWorker` accepts any `worker_id`, host, and port, and registering an existing id replaces its
channel. A rogue process can take over a worker's shards. Bind worker identity to its mTLS certificate and
reject id takeover from a different identity.

### 10.4 Path traversal in shard transfer · P0 · S

`receiveShard` builds the destination as `dataDir.resolve(tableName + "_shard" + shardId + ".duckdb")`
from the client-supplied table name, so a name containing `../` writes outside the data directory.
Validate names with the same pattern `ShardFileMetadata` uses (`^[a-zA-Z_][a-zA-Z0-9_]*$`), check the
normalized path stays under `dataDir`, and add a test that sends a `../` table name to `ReceiveShard`.

### 10.5 Container hardening · P1 · S

The [`Dockerfile`](../Dockerfile) has no `USER` (everything runs as root) and sets no JVM memory flags.
Only the coordinator has a healthcheck in [`docker-compose.yml`](../docker-compose.yml). Run as a non-root
user, set `-XX:MaxRAMPercentage`, and add worker healthchecks.

### 10.6 Lock down the accepted SQL surface · P1 · S

[`FragmentSqlValidator`](../common/src/main/java/io/duckcluster/common/planner/FragmentSqlValidator.java)
checks that fragments start with `SELECT` and reference the right shards. It does not explicitly reject
table functions with file paths (`read_csv`, `read_parquet`), `COPY`, `PRAGMA`, or `ATTACH`. The planner
probably rejects most of these because they aren't catalog tables, but this should be an explicit
allowlist backed by a test suite rather than a side effect.

---

## Suggested order of work

Functionality first; security last.

1. **Correctness and stability:** 1.1–1.5, 2.1, 3.1, 6.2, 9.1. Mostly small changes that remove wrong
   results, hangs, and unbounded resource use.
2. **Operable service:** 2.2, 2.3, 2.4 steps 1–2, 3.2, 3.3, 5.1, 5.3, 6.1, 7.1.
3. **Performance and scale:** 4.1–4.7, 5.2, 6.3, 6.4.
4. **Platform:** 2.4 step 3 (HA coordinator), 7.3, 8.2, 8.3.
5. **Security:** 10.1–10.6. Start with 10.2 and 10.4, the most severe. 10.4, plus the bind-address and
   DuckDB settings from 10.2, are small enough to pull forward if the cluster needs to be reachable beyond
   a trusted network before phase 5.
