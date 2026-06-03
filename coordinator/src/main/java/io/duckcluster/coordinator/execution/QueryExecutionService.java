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
import io.duckcluster.coordinator.merger.MergeStrategyRegistry;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import io.duckcluster.proto.v1.ShardDataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    public QueryExecutionService(
            QueryPlanner planner,
            ClusterCatalog catalog,
            WorkerRegistry registry,
            WorkerNodeClient workerClient,
            MergeStrategyRegistry mergeStrategyRegistry,
            ShardCatalog shardCatalog,
            ClusterConfig config) {
        this(planner, catalog, registry, workerClient, mergeStrategyRegistry, shardCatalog,
                config.fragmentWaitMs(), config.logFragmentSql());
    }

    QueryExecutionService(
            QueryPlanner planner,
            ClusterCatalog catalog,
            WorkerRegistry registry,
            WorkerNodeClient workerClient,
            MergeStrategyRegistry mergeStrategyRegistry,
            ShardCatalog shardCatalog,
            long fragmentWaitMs) {
        this(planner, catalog, registry, workerClient, mergeStrategyRegistry, shardCatalog, fragmentWaitMs, false);
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
        this.planner = planner;
        this.catalog = catalog;
        this.registry = registry;
        this.workerClient = workerClient;
        this.mergeStrategyRegistry = mergeStrategyRegistry;
        this.shardCatalog = shardCatalog;
        this.fragmentTracker = new WorkerFragmentTracker(registry);
        this.fragmentWaitMs = fragmentWaitMs;
        this.logFragmentSql = logFragmentSql;
        this.fragmentExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }

    public QueryResult execute(String sql) {
        long startMs = System.currentTimeMillis();
        String queryId = UUID.randomUUID().toString();
        LOG.info("Query [{}] received: {}", queryId, sql);

        PlannedQuery plan = planner.plan(sql, catalog);
        LOG.info("Query [{}] planned: table={}, fragments={}, broadcast={}, strategy={}",
                queryId, plan.tableName(), plan.fragments().size(),
                plan.broadcastTables().size(), plan.mergeStrategy());

        if (registry.workerCount() == 0) {
            throw new IllegalStateException("No workers registered");
        }

        MergeStrategy mergeStrategy = mergeStrategyRegistry.get(plan.mergeStrategy());

        Map<Integer, String> fragmentTargets = resolveFragmentTargets(plan);
        LOG.debug("Query [{}] targets resolved: {}", queryId, fragmentTargets);

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

        List<CompletableFuture<ExecutionOutcome>> futures = new ArrayList<>(plan.fragments().size());
        for (FragmentSpec fragment : plan.fragments()) {
            if (logFragmentSql) {
                LOG.info("Query [{}] fragment {} shard {} SQL: {}",
                        queryId, fragment.fragmentId(), fragment.shardId(), fragment.sql());
            }
            String preferredWorkerId = fragmentTargets.get(fragment.shardId());
            futures.add(CompletableFuture.supplyAsync(
                    () -> executeFragmentWithRetry(queryId, plan, fragment, preferredWorkerId),
                    fragmentExecutor));
        }

        List<FragmentResult> fragmentResults = new ArrayList<>(plan.fragments().size());
        Map<String, Long> workerDurationsMs = new LinkedHashMap<>();

        for (int i = 0; i < futures.size(); i++) {
            FragmentSpec fragment = plan.fragments().get(i);
            ExecutionOutcome outcome = futures.get(i).join();
            workerDurationsMs.merge(outcome.workerId, outcome.durationMs, Long::sum);
            fragmentResults.add(new FragmentResult(
                    fragment.fragmentId(),
                    fragment.shardId(),
                    outcome.workerId,
                    outcome.durationMs,
                    outcome.batches));
            LOG.debug("Query [{}] fragment {} completed on {} ({}ms)",
                    queryId, fragment.fragmentId(), outcome.workerId, outcome.durationMs);
        }

        long durationMs = System.currentTimeMillis() - startMs;
        Map<String, RowBatchData> coordinatorTables = fetchCoordinatorTables(queryId, plan);
        MergeContext context = new MergeContext(queryId, plan, fragmentResults, durationMs, coordinatorTables);
        QueryResult merged = mergeStrategy.merge(context);
        LOG.info("Query [{}] complete: {}ms, {} fragments, {} workers",
                queryId, durationMs, plan.fragments().size(), workerDurationsMs.size());
        return withTiming(merged, durationMs, workerDurationsMs);
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
            if (registry.getWorker(ownerId).isPresent()) {
                return ownerId;
            }
        }
        for (String workerId : shardCatalog.getCachedWorkers(tableName, shardId)) {
            if (registry.getWorker(workerId).isPresent()) {
                return workerId;
            }
        }
        for (String ownerId : shardCatalog.getActualOwners(tableName, shardId)) {
            if (registry.getWorker(ownerId).isPresent()) {
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
                .min((left, right) -> Integer.compare(
                        fragmentTracker.inFlightCount(left.workerId()),
                        fragmentTracker.inFlightCount(right.workerId())))
                .orElseThrow(() -> new IllegalStateException("No workers registered"))
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
        List<String> sourceOwners = shardCatalog.getActualOwners(tableName, shardId);
        if (sourceOwners.isEmpty()) {
            sourceOwners = shardCatalog.getOwners(tableName, shardId);
        }
        WorkerRegistry.WorkerRecord source = null;
        for (String ownerId : sourceOwners) {
            Optional<WorkerRegistry.WorkerRecord> w = registry.getWorker(ownerId);
            if (w.isPresent()) {
                source = w.get();
                break;
            }
        }
        if (source == null) {
            throw new IllegalStateException(
                    "No source worker available for broadcast shard " + tableName + "_shard" + shardId);
        }

        long transferStart = System.currentTimeMillis();
        LOG.info("Prefetching {}_shard{} from {} to {}",
                tableName, shardId, source.workerId(), target.workerId());

        Iterator<ShardDataChunk> chunks = workerClient.streamShardFrom(source, tableName, shardId);
        boolean loaded = workerClient.loadTempData(target, chunks);
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
            String queryId, PlannedQuery plan, FragmentSpec fragment, String preferredWorkerId) {
        List<String> candidates = buildExecutionCandidates(plan, fragment.shardId(), preferredWorkerId);
        FragmentExecutionException lastRetryable = null;

        for (String workerId : candidates) {
            if (registry.getWorker(workerId).isEmpty()) {
                continue;
            }
            ensureShardsLocal(plan, workerId, fragment.shardId());
            try {
                return tryExecuteOnWorker(queryId, fragment, workerId);
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

    private void ensureShardsLocal(PlannedQuery plan, String workerId, int drivingShardId) {
        Optional<WorkerRegistry.WorkerRecord> targetOpt = registry.getWorker(workerId);
        if (targetOpt.isEmpty()) {
            return;
        }
        WorkerRegistry.WorkerRecord target = targetOpt.get();
        if (!workerHasShard(workerId, plan.tableName(), drivingShardId)) {
            prefetchShard(target, plan.tableName(), drivingShardId);
        }
        for (BroadcastTable broadcastTable : plan.broadcastTables()) {
            for (int shardId = 0; shardId < broadcastTable.shardCount(); shardId++) {
                if (!workerHasShard(workerId, broadcastTable.tableName(), shardId)) {
                    prefetchShard(target, broadcastTable.tableName(), shardId);
                }
            }
        }
        for (BroadcastTable broadcastTable : plan.subqueryBroadcastTables()) {
            for (int shardId = 0; shardId < broadcastTable.shardCount(); shardId++) {
                if (!workerHasShard(workerId, broadcastTable.tableName(), shardId)) {
                    prefetchShard(target, broadcastTable.tableName(), shardId);
                }
            }
        }
        for (String tableName : plan.correlatedCoPartitionTables()) {
            if (!workerHasShard(workerId, tableName, drivingShardId)) {
                prefetchShard(target, tableName, drivingShardId);
            }
        }
    }

    private ExecutionOutcome tryExecuteOnWorker(String queryId, FragmentSpec fragment, String targetWorkerId) {
        fragmentTracker.acquireBlocking(targetWorkerId, fragmentWaitMs);
        try {
            WorkerRegistry.WorkerRecord worker = registry.getWorker(targetWorkerId)
                    .orElseThrow(() -> new IllegalStateException("Target worker " + targetWorkerId + " not available"));
            long fragmentStartMs = System.currentTimeMillis();
            List<RowBatchData> batches = workerClient.executeFragment(worker, queryId, fragment);
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
