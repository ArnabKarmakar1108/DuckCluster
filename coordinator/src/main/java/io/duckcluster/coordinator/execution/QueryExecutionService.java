package io.duckcluster.coordinator.execution;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.MergeStrategy;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.BroadcastTable;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.planner.QueryPlanner;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.merger.MergeExecutionService;
import io.duckcluster.coordinator.merger.MergeStreamRegistry;
import io.duckcluster.coordinator.merger.MergeStrategyRegistry;
import io.duckcluster.coordinator.monitor.QueryActivityTracker;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import io.duckcluster.proto.v1.ShardDataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import io.duckcluster.coordinator.http.ApiErrorCode;
import io.duckcluster.coordinator.http.ApiException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public final class QueryExecutionService {
    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutionService.class);
    private static final long WORKER_WAIT_POLL_MS = 100;

    private final QueryPlanner planner;
    private final ClusterCatalog catalog;
    private final WorkerRegistry registry;
    private final WorkerNodeClient workerClient;
    private final MergeStrategyRegistry mergeStrategyRegistry;
    private final ShardCatalog shardCatalog;
    private final WorkerFragmentTracker fragmentTracker;
    private final long fragmentWaitMs;
    private final boolean logFragmentSql;
    private final ExecutorService fragmentExecutor;
    private final Semaphore fragmentAdmission;
    private final long shardPinLeaseMs;
    private final long queryTimeoutMs;
    private final QueryAdmissionControl admission;
    private final QueryActivityTracker activityTracker;
    private final MergeExecutionService mergeExecutionService;
    private final ConcurrentHashMap<String, ActiveQueryHandle> activeQueries = new ConcurrentHashMap<>();
    private static final int PREFETCH_MAX_ATTEMPTS = 3;

    public QueryExecutionService(
            QueryPlanner planner,
            ClusterCatalog catalog,
            WorkerRegistry registry,
            WorkerNodeClient workerClient,
            MergeStrategyRegistry mergeStrategyRegistry,
            ShardCatalog shardCatalog,
            ClusterConfig config,
            QueryActivityTracker activityTracker,
            QueryAdmissionControl admission) {
        this(
                planner,
                catalog,
                registry,
                workerClient,
                mergeStrategyRegistry,
                shardCatalog,
                config,
                activityTracker,
                admission,
                new MergeExecutionService(config.mergePoolSize()));
    }

    public QueryExecutionService(
            QueryPlanner planner,
            ClusterCatalog catalog,
            WorkerRegistry registry,
            WorkerNodeClient workerClient,
            MergeStrategyRegistry mergeStrategyRegistry,
            ShardCatalog shardCatalog,
            ClusterConfig config,
            QueryActivityTracker activityTracker,
            QueryAdmissionControl admission,
            MergeExecutionService mergeExecutionService) {
        this(planner, catalog, registry, workerClient, mergeStrategyRegistry, shardCatalog,
                config.fragmentWaitMs(), config.logFragmentSql(), config.shardPinLeaseMs(),
                config.maxConcurrentFragments(), config.queryTimeoutMs(), activityTracker, admission,
                mergeExecutionService);
    }

    QueryExecutionService(
            QueryPlanner planner,
            ClusterCatalog catalog,
            WorkerRegistry registry,
            WorkerNodeClient workerClient,
            MergeStrategyRegistry mergeStrategyRegistry,
            ShardCatalog shardCatalog,
            long fragmentWaitMs) {
        this(planner, catalog, registry, workerClient, mergeStrategyRegistry, shardCatalog, fragmentWaitMs, false,
                TimeUnit.MINUTES.toMillis(10), 64, 0, null, new QueryAdmissionControl(32),
                new MergeExecutionService(4));
    }

    QueryExecutionService(
            QueryPlanner planner,
            ClusterCatalog catalog,
            WorkerRegistry registry,
            WorkerNodeClient workerClient,
            MergeStrategyRegistry mergeStrategyRegistry,
            ShardCatalog shardCatalog,
            long fragmentWaitMs,
            boolean logFragmentSql) {
        this(planner, catalog, registry, workerClient, mergeStrategyRegistry, shardCatalog,
                fragmentWaitMs, logFragmentSql, TimeUnit.MINUTES.toMillis(10), 64, 0, null,
                new QueryAdmissionControl(32), new MergeExecutionService(4));
    }

    QueryExecutionService(
            QueryPlanner planner,
            ClusterCatalog catalog,
            WorkerRegistry registry,
            WorkerNodeClient workerClient,
            MergeStrategyRegistry mergeStrategyRegistry,
            ShardCatalog shardCatalog,
            long fragmentWaitMs,
            boolean logFragmentSql,
            long shardPinLeaseMs,
            int maxConcurrentFragments,
            long queryTimeoutMs,
            QueryActivityTracker activityTracker,
            QueryAdmissionControl admission,
            MergeExecutionService mergeExecutionService) {
        this.planner = planner;
        this.catalog = catalog;
        this.registry = registry;
        this.workerClient = workerClient;
        this.mergeStrategyRegistry = mergeStrategyRegistry;
        this.shardCatalog = shardCatalog;
        this.fragmentTracker = new WorkerFragmentTracker(registry);
        this.fragmentWaitMs = fragmentWaitMs;
        this.logFragmentSql = logFragmentSql;
        this.shardPinLeaseMs = shardPinLeaseMs;
        this.queryTimeoutMs = queryTimeoutMs;
        this.admission = admission;
        this.activityTracker = activityTracker;
        this.mergeExecutionService = mergeExecutionService;
        int poolSize = Math.max(1, maxConcurrentFragments);
        this.fragmentAdmission = new Semaphore(poolSize);
        this.fragmentExecutor = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "query-fragment");
            t.setDaemon(true);
            return t;
        });
    }

    public double workerLoad(String workerId) {
        return registry.getWorker(workerId)
                .map(worker -> (double) fragmentTracker.inFlightCount(workerId)
                        / Math.max(1, worker.numThreads()))
                .orElse(0.0);
    }

    public PlannedQuery explainPlan(String sql) {
        try {
            return planner.plan(sql, catalog, UUID.randomUUID().toString());
        } catch (IllegalArgumentException planningError) {
            throw new ApiException(ApiErrorCode.PLANNING_ERROR, planningError.getMessage(), planningError);
        }
    }

    public QueryResult execute(String sql) {
        return execute(UUID.randomUUID().toString(), sql);
    }

    public QueryResult execute(String queryId, String sql) {
        return execute(queryId, sql, true);
    }

    public QueryResult execute(String queryId, String sql, boolean acquireAdmission) {
        return execute(queryId, sql, acquireAdmission, null);
    }

    public QueryResult execute(String queryId, String sql, boolean acquireAdmission, QueryResultSpool spool) {
        if (acquireAdmission && !admission.tryAcquire()) {
            throw new QueryRejectedException("too many concurrent queries");
        }
        long startMs = System.currentTimeMillis();
        long deadlineMs = queryTimeoutMs > 0 ? startMs + queryTimeoutMs : Long.MAX_VALUE;
        LOG.info("Query [{}] received: {}", queryId, sql);
        MDC.put("queryId", queryId);
        trackStart(queryId, sql);
        ActiveQueryHandle activeQuery = new ActiveQueryHandle();
        activeQueries.put(queryId, activeQuery);

        try {
            ensureNotCancelled(queryId);
            trackPhase(queryId, QueryActivityTracker.Phase.PLAN);
            PlannedQuery plan;
            try {
                plan = planner.plan(sql, catalog, queryId);
            } catch (IllegalArgumentException planningError) {
                throw new ApiException(ApiErrorCode.PLANNING_ERROR, planningError.getMessage(), planningError);
            }
            trackFragmentsTotal(queryId, plan.fragments().size());
            LOG.info("Query [{}] planned: table={}, fragments={}, broadcast={}, strategy={}",
                    queryId, plan.tableName(), plan.fragments().size(),
                    plan.broadcastTables().size(), plan.mergeStrategy());

            if (registry.workerCount() == 0) {
                throw new ApiException(ApiErrorCode.NO_WORKERS, "No workers registered");
            }

            MergeStrategy mergeStrategy = mergeStrategyRegistry.get(plan.mergeStrategy());

            Map<Integer, String> fragmentTargets = resolveFragmentTargets(plan);
            LOG.debug("Query [{}] targets resolved: {}", queryId, fragmentTargets);

            trackPhase(queryId, QueryActivityTracker.Phase.PREFETCH);
            if (!plan.broadcastTables().isEmpty() || !plan.subqueryBroadcastTables().isEmpty()) {
            Set<String> targetWorkerIds = new HashSet<>(fragmentTargets.values());
            long prefetchStart = System.currentTimeMillis();
            int prefetchShards = plan.broadcastTables().stream().mapToInt(BroadcastTable::shardCount).sum()
                    + plan.subqueryBroadcastTables().stream().mapToInt(BroadcastTable::shardCount).sum();
            LOG.info("Query [{}] prefetching {} broadcast/subquery shards to {} workers",
                    queryId, prefetchShards, targetWorkerIds.size());
            prefetchBroadcastShards(plan.broadcastTables(), targetWorkerIds);
            prefetchBroadcastShards(plan.subqueryBroadcastTables(), targetWorkerIds);
            LOG.info("Query [{}] prefetch complete ({}ms)", queryId, System.currentTimeMillis() - prefetchStart);
        } else {
            long prefetchStart = System.currentTimeMillis();
            prefetchDrivingShards(plan, fragmentTargets);
            LOG.info("Query [{}] driving-shard prefetch complete ({}ms)",
                    queryId, System.currentTimeMillis() - prefetchStart);
        }

        Set<String> preparedWorkers = prepareWorkersForQuery(queryId, plan, fragmentTargets);
        activeQuery.setPreparedWorkers(preparedWorkers);
        boolean streamMergeToDuckDb = MergeStreamRegistry.tryStart(queryId, plan);
        try {
            ensureNotCancelled(queryId);
            trackPhase(queryId, QueryActivityTracker.Phase.FRAGMENTS);
            List<CompletableFuture<ExecutionOutcome>> futures = new ArrayList<>(plan.fragments().size());
            for (FragmentSpec fragment : plan.fragments()) {
                if (logFragmentSql) {
                    LOG.info("Query [{}] fragment {} shard {} SQL: {}",
                            queryId, fragment.fragmentId(), fragment.shardId(), fragment.sql());
                }
                String preferredWorkerId = fragmentTargets.get(fragment.shardId());
                CompletableFuture<ExecutionOutcome> future = CompletableFuture.supplyAsync(
                        () -> runFragmentWithAdmission(
                                queryId, plan, fragment, preferredWorkerId, preparedWorkers, streamMergeToDuckDb),
                        fragmentExecutor);
                activeQuery.trackFuture(future);
                futures.add(future);
            }

            List<FragmentResult> fragmentResults = new ArrayList<>(plan.fragments().size());
            Map<String, Long> workerDurationsMs = new LinkedHashMap<>();

            for (int i = 0; i < futures.size(); i++) {
                ensureNotCancelled(queryId);
                FragmentSpec fragment = plan.fragments().get(i);
                long remainingMs = deadlineMs - System.currentTimeMillis();
                if (remainingMs <= 0) {
                    futures.forEach(future -> future.cancel(true));
                    throw new QueryTimeoutException("query timed out after " + queryTimeoutMs + "ms");
                }
                ExecutionOutcome outcome;
                try {
                    outcome = futures.get(i).get(remainingMs, TimeUnit.MILLISECONDS);
                } catch (java.util.concurrent.TimeoutException e) {
                    futures.forEach(future -> future.cancel(true));
                    throw new QueryTimeoutException("query timed out waiting for fragment " + fragment.fragmentId());
                } catch (java.util.concurrent.ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    throw new IllegalStateException(cause);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted waiting for fragment", e);
                }
                workerDurationsMs.merge(outcome.workerId, outcome.durationMs, Long::sum);
                fragmentResults.add(new FragmentResult(
                        fragment.fragmentId(),
                        fragment.shardId(),
                        outcome.workerId,
                        outcome.durationMs,
                        outcome.batches));
                if (spool != null && plan.mergeStrategy() == MergeStrategyType.CONCATENATE) {
                    for (RowBatchData batch : outcome.batches) {
                        spool.appendBatch(batch);
                    }
                }
                LOG.debug("Query [{}] fragment {} completed on {} ({}ms)",
                        queryId, fragment.fragmentId(), outcome.workerId, outcome.durationMs);
                trackFragmentDone(queryId);
            }

            long durationMs = System.currentTimeMillis() - startMs;
            if (spool != null && plan.mergeStrategy() == MergeStrategyType.CONCATENATE) {
                QueryResult.QueryStats stats = new QueryResult.QueryStats(
                        MergeStrategyType.CONCATENATE,
                        workerDurationsMs.size(),
                        fragmentResults.size(),
                        durationMs,
                        workerDurationsMs);
                QueryResult streamed = new QueryResult(queryId, spool.columns(), List.of(), stats);
                LOG.info("Query [{}] complete (streamed concat): {}ms, {} fragments, {} workers",
                        queryId, durationMs, plan.fragments().size(), workerDurationsMs.size());
                QueryResult timed = withTiming(streamed, durationMs, workerDurationsMs);
                trackComplete(queryId, timed);
                return timed;
            }

            Map<String, RowBatchData> coordinatorTables = fetchCoordinatorTables(queryId, plan);
            MergeContext context = new MergeContext(queryId, plan, fragmentResults, durationMs, coordinatorTables);
            trackPhase(queryId, QueryActivityTracker.Phase.MERGE);
            QueryResult merged = mergeExecutionService.run(() -> mergeStrategy.merge(context));
            if (spool != null && !merged.rows().isEmpty()) {
                spool.writeAll(merged.columns(), merged.rows());
                merged = new QueryResult(merged.queryId(), merged.columns(), List.of(), merged.stats());
            }
            LOG.info("Query [{}] complete: {}ms, {} fragments, {} workers",
                    queryId, durationMs, plan.fragments().size(), workerDurationsMs.size());
            QueryResult timed = withTiming(merged, durationMs, workerDurationsMs);
            trackComplete(queryId, timed);
            return timed;
        } finally {
            MergeStreamRegistry.remove(queryId);
            releasePreparedWorkers(queryId, preparedWorkers);
        }
        } catch (RuntimeException e) {
            trackFail(queryId, e.getMessage(), System.currentTimeMillis() - startMs);
            throw e;
        } finally {
            activeQueries.remove(queryId);
            if (acquireAdmission) {
                admission.release();
            }
            MDC.remove("queryId");
        }
    }

    public void awaitDrain(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!activeQueries.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    public boolean cancelQuery(String queryId) {
        ActiveQueryHandle handle = activeQueries.get(queryId);
        if (handle == null) {
            return false;
        }
        handle.cancel();
        for (String workerId : handle.preparedWorkers()) {
            registry.getWorker(workerId).ifPresent(worker -> workerClient.cancelQuery(worker, queryId));
        }
        if (!handle.preparedWorkers().isEmpty()) {
            releasePreparedWorkers(queryId, new LinkedHashSet<>(handle.preparedWorkers()));
        }
        return true;
    }

    private void ensureNotCancelled(String queryId) {
        ActiveQueryHandle handle = activeQueries.get(queryId);
        if (handle != null && handle.isCancelled()) {
            throw new QueryCancelledException(queryId);
        }
    }

    private ExecutionOutcome runFragmentWithAdmission(
            String queryId,
            PlannedQuery plan,
            FragmentSpec fragment,
            String preferredWorkerId,
            Set<String> preparedWorkers,
            boolean streamMergeToDuckDb) {
        ensureNotCancelled(queryId);
        try {
            fragmentAdmission.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for fragment admission slot", e);
        }
        try {
            return executeFragmentWithRetry(
                    queryId, plan, fragment, preferredWorkerId, preparedWorkers, streamMergeToDuckDb);
        } finally {
            fragmentAdmission.release();
        }
    }

    private void trackStart(String queryId, String sql) {
        if (activityTracker != null) {
            activityTracker.start(queryId, sql);
        }
    }

    private void trackPhase(String queryId, QueryActivityTracker.Phase phase) {
        if (activityTracker != null) {
            activityTracker.setPhase(queryId, phase);
        }
    }

    private void trackFragmentsTotal(String queryId, int total) {
        if (activityTracker != null) {
            activityTracker.setFragmentsTotal(queryId, total);
        }
    }

    private void trackFragmentDone(String queryId) {
        if (activityTracker != null) {
            activityTracker.fragmentDone(queryId);
        }
    }

    private void trackComplete(String queryId, QueryResult result) {
        if (activityTracker != null) {
            activityTracker.complete(queryId, result);
        }
    }

    private void trackFail(String queryId, String error, long durationMs) {
        if (activityTracker != null) {
            activityTracker.fail(queryId, error, durationMs);
        }
    }

    private Map<Integer, String> resolveFragmentTargets(PlannedQuery plan) {
        boolean joinQuery = !plan.broadcastTables().isEmpty();
        Map<Integer, String> targets = new LinkedHashMap<>();
        for (FragmentSpec fragment : plan.fragments()) {
            String resolved = resolveRegisteredCandidate(plan.tableName(), fragment.shardId());
            if (resolved == null) {
                if (joinQuery) {
                    resolved = waitForOwnerOrCachedWorker(plan.tableName(), fragment.shardId());
                } else {
                    resolved = pickFallbackWorker();
                    LOG.info("No registered owner/cache for {}_shard{}, using fallback worker {}",
                            plan.tableName(), fragment.shardId(), resolved);
                }
            }
            targets.put(fragment.shardId(), resolved);
        }
        return targets;
    }

    private String resolveRegisteredCandidate(String tableName, int shardId) {
        for (String ownerId : shardCatalog.getOwners(tableName, shardId)) {
            if (registry.isSchedulable(ownerId)) {
                return ownerId;
            }
        }
        for (String workerId : shardCatalog.getCachedWorkers(tableName, shardId)) {
            if (registry.isSchedulable(workerId)) {
                return workerId;
            }
        }
        for (String ownerId : shardCatalog.getActualOwners(tableName, shardId)) {
            if (registry.isSchedulable(ownerId)) {
                return ownerId;
            }
        }
        return null;
    }

    private String waitForOwnerOrCachedWorker(String tableName, int shardId) {
        long deadline = System.currentTimeMillis() + fragmentWaitMs;
        while (System.currentTimeMillis() < deadline) {
            String resolved = resolveRegisteredCandidate(tableName, shardId);
            if (resolved != null) {
                LOG.info("Resolved {}_shard{} to owner/cache worker {} after waiting",
                        tableName, shardId, resolved);
                return resolved;
            }
            sleepQuietly(WORKER_WAIT_POLL_MS);
        }
        throw new IllegalStateException(
                "Timed out waiting for an owner or cached worker for " + tableName + "_shard" + shardId);
    }

    private String pickFallbackWorker() {
        return registry.listWorkers().stream()
                .filter(worker -> worker.status() == WorkerRegistry.WorkerStatus.HEALTHY)
                .min((left, right) -> Integer.compare(
                        fragmentTracker.inFlightCount(left.workerId()),
                        fragmentTracker.inFlightCount(right.workerId())))
                .orElseThrow(() -> new IllegalStateException("No healthy workers registered"))
                .workerId();
    }

    private void prefetchDrivingShards(PlannedQuery plan, Map<Integer, String> fragmentTargets) {
        List<CompletableFuture<Void>> prefetchFutures = new ArrayList<>();
        for (FragmentSpec fragment : plan.fragments()) {
            String targetWorkerId = fragmentTargets.get(fragment.shardId());
            if (workerHasShard(targetWorkerId, plan.tableName(), fragment.shardId())) {
                continue;
            }
            Optional<WorkerRegistry.WorkerRecord> targetOpt = registry.getWorker(targetWorkerId);
            if (targetOpt.isEmpty()) {
                continue;
            }
            WorkerRegistry.WorkerRecord target = targetOpt.get();
            final int shardId = fragment.shardId();
            prefetchFutures.add(CompletableFuture.runAsync(
                    () -> prefetchShard(target, plan.tableName(), shardId), fragmentExecutor));
        }
        for (CompletableFuture<Void> future : prefetchFutures) {
            future.join();
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for shard worker", e);
        }
    }

    private void prefetchBroadcastShards(List<BroadcastTable> broadcastTables, Set<String> targetWorkerIds) {
        List<CompletableFuture<Void>> prefetchFutures = new ArrayList<>();

        for (String targetWorkerId : targetWorkerIds) {
            Optional<WorkerRegistry.WorkerRecord> targetOpt = registry.getWorker(targetWorkerId);
            if (targetOpt.isEmpty()) continue;
            WorkerRegistry.WorkerRecord target = targetOpt.get();

            for (BroadcastTable bt : broadcastTables) {
                for (int shardId = 0; shardId < bt.shardCount(); shardId++) {
                    if (workerHasShard(targetWorkerId, bt.tableName(), shardId)) {
                        continue;
                    }
                    final int sid = shardId;
                    prefetchFutures.add(CompletableFuture.runAsync(() ->
                            prefetchShard(target, bt.tableName(), sid), fragmentExecutor));
                }
            }
        }

        for (CompletableFuture<Void> f : prefetchFutures) {
            f.join();
        }
    }

    private boolean workerHasShard(String workerId, String tableName, int shardId) {
        List<String> actualOwners = shardCatalog.getActualOwners(tableName, shardId);
        if (actualOwners.contains(workerId)) return true;
        List<String> cachedWorkers = shardCatalog.getCachedWorkers(tableName, shardId);
        return cachedWorkers.contains(workerId);
    }

    private void prefetchShard(WorkerRegistry.WorkerRecord target, String tableName, int shardId) {
        prefetchShard(target, tableName, shardId, null);
    }

    private void prefetchShard(WorkerRegistry.WorkerRecord target, String tableName, int shardId, String queryId) {
        WorkerRegistry.WorkerRecord source = findSourceWorker(tableName, shardId);
        long transferStart = System.currentTimeMillis();
        LOG.info("Prefetching {}_shard{} from {} to {}",
                tableName, shardId, source.workerId(), target.workerId());

        boolean loaded = prefetchWithRetry(target, tableName, shardId, queryId);
        if (!loaded) {
            throw new IllegalStateException(
                    "Failed to prefetch broadcast shard " + tableName + "_shard" + shardId
                            + " to worker " + target.workerId());
        }
        LOG.info("Prefetched {}_shard{} from {} to {} ({}ms)",
                tableName, shardId, source.workerId(), target.workerId(),
                System.currentTimeMillis() - transferStart);
    }

    private ExecutionOutcome executeFragmentWithRetry(
            String queryId,
            PlannedQuery plan,
            FragmentSpec fragment,
            String preferredWorkerId,
            Set<String> preparedWorkers,
            boolean streamMergeToDuckDb) {
        List<String> candidates = buildExecutionCandidates(plan, fragment.shardId(), preferredWorkerId);
        FragmentExecutionException lastRetryable = null;

        for (String workerId : candidates) {
            if (!registry.isSchedulable(workerId)) {
                continue;
            }
            try {
                ensureWorkerPrepared(
                        queryId, plan, workerId, List.of(fragment.shardId()), preparedWorkers);
                return tryExecuteOnWorker(queryId, fragment, workerId, streamMergeToDuckDb);
            } catch (FragmentExecutionException e) {
                if (e.retryable()) {
                    LOG.warn("Query [{}] fragment {} failed on {} ({}), trying next worker",
                            queryId, fragment.fragmentId(), workerId, e.status().getCode());
                    lastRetryable = e;
                } else {
                    throw e;
                }
            }
        }

        if (lastRetryable != null) {
            throw new IllegalStateException(
                    "All candidate workers failed for fragment " + fragment.fragmentId()
                            + " on table " + plan.tableName() + "_shard" + fragment.shardId(),
                    lastRetryable);
        }
        throw new IllegalStateException(
                "No candidate workers available for fragment " + fragment.fragmentId());
    }

    private List<String> buildExecutionCandidates(PlannedQuery plan, int shardId, String preferredWorkerId) {
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        if (preferredWorkerId != null) {
            candidates.add(preferredWorkerId);
        }
        for (String ownerId : shardCatalog.getOwners(plan.tableName(), shardId)) {
            if (registry.getWorker(ownerId).isPresent()) {
                candidates.add(ownerId);
            }
        }
        for (String ownerId : shardCatalog.getActualOwners(plan.tableName(), shardId)) {
            if (registry.getWorker(ownerId).isPresent()) {
                candidates.add(ownerId);
            }
        }
        for (String workerId : shardCatalog.getCachedWorkers(plan.tableName(), shardId)) {
            if (registry.getWorker(workerId).isPresent()) {
                candidates.add(workerId);
            }
        }
        if (plan.broadcastTables().isEmpty()) {
            registry.listWorkers().stream()
                    .sorted((left, right) -> Integer.compare(
                            fragmentTracker.inFlightCount(left.workerId()),
                            fragmentTracker.inFlightCount(right.workerId())))
                    .forEach(worker -> candidates.add(worker.workerId()));
        }
        return List.copyOf(candidates);
    }

    private void ensureShardsLocal(String queryId, PlannedQuery plan, String workerId, int drivingShardId) {
        Optional<WorkerRegistry.WorkerRecord> targetOpt = registry.getWorker(workerId);
        if (targetOpt.isEmpty()) {
            return;
        }
        WorkerRegistry.WorkerRecord target = targetOpt.get();
        if (!workerHasShard(workerId, plan.tableName(), drivingShardId)) {
            prefetchShard(target, plan.tableName(), drivingShardId, queryId);
        }
        for (BroadcastTable broadcastTable : plan.broadcastTables()) {
            for (int shardId = 0; shardId < broadcastTable.shardCount(); shardId++) {
                if (!workerHasShard(workerId, broadcastTable.tableName(), shardId)) {
                    prefetchShard(target, broadcastTable.tableName(), shardId, queryId);
                }
            }
        }
        for (BroadcastTable broadcastTable : plan.subqueryBroadcastTables()) {
            for (int shardId = 0; shardId < broadcastTable.shardCount(); shardId++) {
                if (!workerHasShard(workerId, broadcastTable.tableName(), shardId)) {
                    prefetchShard(target, broadcastTable.tableName(), shardId, queryId);
                }
            }
        }
        for (String tableName : plan.correlatedCoPartitionTables()) {
            if (!workerHasShard(workerId, tableName, drivingShardId)) {
                prefetchShard(target, tableName, drivingShardId, queryId);
            }
        }
    }

    private Set<String> prepareWorkersForQuery(
            String queryId, PlannedQuery plan, Map<Integer, String> fragmentTargets) {
        Map<String, List<Integer>> shardIdsByWorker = new LinkedHashMap<>();
        for (FragmentSpec fragment : plan.fragments()) {
            String workerId = fragmentTargets.get(fragment.shardId());
            shardIdsByWorker.computeIfAbsent(workerId, ignored -> new ArrayList<>())
                    .add(fragment.shardId());
        }

        Set<String> preparedWorkers = Collections.synchronizedSet(new LinkedHashSet<>());
        List<CompletableFuture<Void>> prepareFutures = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : shardIdsByWorker.entrySet()) {
            prepareFutures.add(CompletableFuture.runAsync(
                    () -> ensureWorkerPrepared(queryId, plan, entry.getKey(), entry.getValue(), preparedWorkers),
                    fragmentExecutor));
        }
        for (CompletableFuture<Void> future : prepareFutures) {
            future.join();
        }
        return preparedWorkers;
    }

    private boolean prefetchWithRetry(
            WorkerRegistry.WorkerRecord target, String tableName, int shardId, String queryId) {
        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= PREFETCH_MAX_ATTEMPTS; attempt++) {
            try {
                Iterator<ShardDataChunk> chunks = workerClient.streamShardFrom(
                        findSourceWorker(tableName, shardId), tableName, shardId);
                if (workerClient.loadTempData(target, chunks, queryId)) {
                    return true;
                }
            } catch (RuntimeException e) {
                lastFailure = e;
                LOG.warn(
                        "Prefetch attempt {}/{} failed for {}_shard{} -> {}: {}",
                        attempt,
                        PREFETCH_MAX_ATTEMPTS,
                        tableName,
                        shardId,
                        target.workerId(),
                        e.getMessage());
            }
            sleepQuietly(50L * attempt);
        }
        if (lastFailure != null) {
            throw lastFailure;
        }
        return false;
    }

    private WorkerRegistry.WorkerRecord findSourceWorker(String tableName, int shardId) {
        List<String> sourceOwners = shardCatalog.getActualOwners(tableName, shardId);
        if (sourceOwners.isEmpty()) {
            sourceOwners = shardCatalog.getOwners(tableName, shardId);
        }
        for (String ownerId : sourceOwners) {
            Optional<WorkerRegistry.WorkerRecord> worker = registry.getWorker(ownerId);
            if (worker.isPresent()) {
                return worker.get();
            }
        }
        throw new IllegalStateException(
                "No source worker available for shard " + tableName + "_shard" + shardId);
    }

    private void ensureWorkerPrepared(
            String queryId,
            PlannedQuery plan,
            String workerId,
            Collection<Integer> shardIds,
            Set<String> preparedWorkers) {
        if (preparedWorkers.contains(workerId)) {
            return;
        }
        synchronized (preparedWorkers) {
            if (preparedWorkers.contains(workerId)) {
                return;
            }
            WorkerRegistry.WorkerRecord worker = registry.getWorker(workerId)
                    .orElseThrow(() -> new IllegalStateException("Worker " + workerId + " not available"));
            long leaseTtlSec = TimeUnit.MILLISECONDS.toSeconds(shardPinLeaseMs);
            workerClient.beginShardPin(worker, queryId, leaseTtlSec);
            for (int shardId : shardIds) {
                ensureShardsLocal(queryId, plan, workerId, shardId);
            }
            for (BroadcastTable broadcastTable : broadcastTablesToMaterialize(plan)) {
                workerClient.materializeBroadcast(
                        worker, queryId, broadcastTable.tableName(), broadcastTable.shardCount());
            }
            preparedWorkers.add(workerId);
        }
    }

    private void releasePreparedWorkers(String queryId, Set<String> preparedWorkers) {
        for (String workerId : preparedWorkers) {
            try {
                Optional<WorkerRegistry.WorkerRecord> workerOpt = registry.getWorker(workerId);
                if (workerOpt.isEmpty()) {
                    continue;
                }
                WorkerRegistry.WorkerRecord worker = workerOpt.get();
                try {
                    workerClient.clearBroadcastTables(worker, queryId);
                } catch (Exception e) {
                    LOG.warn("Query [{}] failed to clear broadcast tables on {}: {}",
                            queryId, workerId, e.getMessage());
                }
                try {
                    workerClient.endShardPin(worker, queryId);
                } catch (Exception e) {
                    LOG.warn("Query [{}] failed to end shard pin on {}: {}",
                            queryId, workerId, e.getMessage());
                }
            } catch (Exception e) {
                LOG.warn("Query [{}] worker cleanup failed for {}: {}", queryId, workerId, e.getMessage());
            }
        }
    }

    private static List<BroadcastTable> broadcastTablesToMaterialize(PlannedQuery plan) {
        Map<String, BroadcastTable> tables = new LinkedHashMap<>();
        for (BroadcastTable broadcastTable : plan.broadcastTables()) {
            if (broadcastTable.shardCount() > 1) {
                tables.putIfAbsent(broadcastTable.tableName().toLowerCase(), broadcastTable);
            }
        }
        for (BroadcastTable broadcastTable : plan.subqueryBroadcastTables()) {
            if (broadcastTable.shardCount() > 1) {
                tables.putIfAbsent(broadcastTable.tableName().toLowerCase(), broadcastTable);
            }
        }
        return List.copyOf(tables.values());
    }

    private ExecutionOutcome tryExecuteOnWorker(
            String queryId, FragmentSpec fragment, String targetWorkerId, boolean streamMergeToDuckDb) {
        fragmentTracker.acquireBlocking(targetWorkerId, fragmentWaitMs);
        try {
            WorkerRegistry.WorkerRecord worker = registry.getWorker(targetWorkerId)
                    .orElseThrow(() -> new IllegalStateException("Target worker " + targetWorkerId + " not available"));
            long fragmentStartMs = System.currentTimeMillis();
            List<RowBatchData> batches;
            if (streamMergeToDuckDb) {
                workerClient.consumeFragmentBatches(
                        worker,
                        queryId,
                        fragment,
                        batch -> MergeStreamRegistry.appendBatch(queryId, fragment.fragmentId(), batch));
                batches = List.of();
            } else {
                batches = workerClient.executeFragment(worker, queryId, fragment);
            }
            long fragmentDurationMs = System.currentTimeMillis() - fragmentStartMs;
            return new ExecutionOutcome(targetWorkerId, fragmentDurationMs, batches);
        } finally {
            fragmentTracker.release(targetWorkerId);
        }
    }


    private Map<String, RowBatchData> fetchCoordinatorTables(String queryId, PlannedQuery plan) {
        if (!plan.hasWithCte()) {
            return Map.of();
        }
        Map<String, RowBatchData> tables = new LinkedHashMap<>();
        for (String tableName : plan.withCte().coordinatorDimensionTables()) {
            String workerId = resolveRegisteredCandidate(tableName, 0);
            if (workerId == null) {
                workerId = pickFallbackWorker();
            }
            WorkerRegistry.WorkerRecord worker = registry.getWorker(workerId)
                    .orElseThrow(() -> new IllegalStateException("No worker available for coordinator table " + tableName));
            String sql = "SELECT * FROM \"" + tableName + "_shard0\".\"" + tableName + "\"";
            FragmentSpec fragment = new FragmentSpec(-1, 0, sql, MergeStrategyType.CONCATENATE);
            List<RowBatchData> batches = workerClient.executeFragment(worker, queryId + "-coord-" + tableName, fragment);
            if (batches.isEmpty()) {
                throw new IllegalStateException("Coordinator table fetch returned no rows for " + tableName);
            }
            tables.put(tableName, batches.get(0));
        }
        return tables;
    }

    private record ExecutionOutcome(String workerId, long durationMs, List<RowBatchData> batches) {}

    private static QueryResult withTiming(
            QueryResult result, long durationMs, Map<String, Long> workerDurationsMs) {
        QueryResult.QueryStats stats = new QueryResult.QueryStats(
                result.stats().mergeStrategy(),
                result.stats().workersUsed(),
                result.stats().fragmentsExecuted(),
                durationMs,
                workerDurationsMs);
        return new QueryResult(result.queryId(), result.columns(), result.rows(), stats);
    }
}
