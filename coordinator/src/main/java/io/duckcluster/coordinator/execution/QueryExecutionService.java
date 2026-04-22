package io.duckcluster.coordinator.execution;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.MergeStrategy;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.BroadcastTable;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.planner.QueryPlanner;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.merger.MergeStrategyRegistry;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import io.duckcluster.proto.v1.ShardDataChunk;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final QueryPlanner planner;
    private final ClusterCatalog catalog;
    private final WorkerRegistry registry;
    private final WorkerNodeClient workerClient;
    private final MergeStrategyRegistry mergeStrategyRegistry;
    private final ShardCatalog shardCatalog;
    private final ExecutorService fragmentExecutor;

    public QueryExecutionService(
            QueryPlanner planner,
            ClusterCatalog catalog,
            WorkerRegistry registry,
            WorkerNodeClient workerClient,
            MergeStrategyRegistry mergeStrategyRegistry,
            ShardCatalog shardCatalog) {
        this.planner = planner;
        this.catalog = catalog;
        this.registry = registry;
        this.workerClient = workerClient;
        this.mergeStrategyRegistry = mergeStrategyRegistry;
        this.shardCatalog = shardCatalog;
        this.fragmentExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }

    public QueryResult execute(String sql) {
        long startMs = System.currentTimeMillis();
        String queryId = UUID.randomUUID().toString();
        PlannedQuery plan = planner.plan(sql, catalog);

        if (registry.workerCount() == 0) {
            throw new IllegalStateException("No workers registered");
        }

        MergeStrategy mergeStrategy = mergeStrategyRegistry.get(plan.mergeStrategy());

        // Step 1: Resolve target workers for each fragment (driving table owners)
        Map<Integer, String> fragmentTargets = resolveFragmentTargets(plan);

        // Step 2: Prefetch broadcast shards to resolved target workers
        if (!plan.broadcastTables().isEmpty()) {
            Set<String> targetWorkerIds = new HashSet<>(fragmentTargets.values());
            prefetchBroadcastShards(plan.broadcastTables(), targetWorkerIds);
        }

        // Step 3: Dispatch fragments to resolved targets
        List<CompletableFuture<ExecutionOutcome>> futures = new ArrayList<>(plan.fragments().size());
        for (FragmentSpec fragment : plan.fragments()) {
            String targetWorkerId = fragmentTargets.get(fragment.shardId());
            futures.add(CompletableFuture.supplyAsync(
                    () -> executeOnWorker(queryId, fragment, targetWorkerId), fragmentExecutor));
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
        }

        long durationMs = System.currentTimeMillis() - startMs;
        MergeContext context = new MergeContext(queryId, plan, fragmentResults, durationMs);
        QueryResult merged = mergeStrategy.merge(context);
        return withTiming(merged, durationMs, workerDurationsMs);
    }

    private Map<Integer, String> resolveFragmentTargets(PlannedQuery plan) {
        Map<Integer, String> targets = new LinkedHashMap<>();
        for (FragmentSpec fragment : plan.fragments()) {
            List<String> owners = shardCatalog.getOwners(plan.tableName(), fragment.shardId());
            String resolved = null;
            for (String ownerId : owners) {
                if (registry.getWorker(ownerId).isPresent()) {
                    resolved = ownerId;
                    break;
                }
            }
            if (resolved == null) {
                List<String> cachedWorkers = shardCatalog.getCachedWorkers(plan.tableName(), fragment.shardId());
                for (String workerId : cachedWorkers) {
                    if (registry.getWorker(workerId).isPresent()) {
                        resolved = workerId;
                        break;
                    }
                }
            }
            if (resolved == null) {
                resolved = registry.listWorkers().iterator().next().workerId();
            }
            targets.put(fragment.shardId(), resolved);
        }
        return targets;
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

        LOG.info("Prefetching {}_shard{} from {} to {}",
                tableName, shardId, source.workerId(), target.workerId());

        Iterator<ShardDataChunk> chunks = workerClient.streamShardFrom(source, tableName, shardId);
        boolean loaded = workerClient.loadTempData(target, chunks);
        if (!loaded) {
            LOG.warn("Failed to prefetch {}_shard{} to {}", tableName, shardId, target.workerId());
        }
    }

    private ExecutionOutcome executeOnWorker(String queryId, FragmentSpec fragment, String targetWorkerId) {
        Optional<WorkerRegistry.WorkerRecord> workerOpt = registry.getWorker(targetWorkerId);
        if (workerOpt.isEmpty()) {
            throw new IllegalStateException("Target worker " + targetWorkerId + " not available");
        }
        WorkerRegistry.WorkerRecord worker = workerOpt.get();
        long fragmentStartMs = System.currentTimeMillis();
        List<RowBatchData> batches = workerClient.executeFragment(worker, queryId, fragment);
        long fragmentDurationMs = System.currentTimeMillis() - fragmentStartMs;
        return new ExecutionOutcome(targetWorkerId, fragmentDurationMs, batches);
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
