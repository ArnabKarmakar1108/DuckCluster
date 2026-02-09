package io.duckcluster.coordinator.execution;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.MergeStrategy;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.planner.QueryPlanner;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.merger.MergeStrategyRegistry;
import io.duckcluster.coordinator.worker.WorkerNodeClient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class QueryExecutionService {
    private final QueryPlanner planner;
    private final ClusterCatalog catalog;
    private final WorkerRegistry registry;
    private final WorkerNodeClient workerClient;
    private final MergeStrategyRegistry mergeStrategyRegistry;

    public QueryExecutionService(
            QueryPlanner planner,
            ClusterCatalog catalog,
            WorkerRegistry registry,
            WorkerNodeClient workerClient,
            MergeStrategyRegistry mergeStrategyRegistry) {
        this.planner = planner;
        this.catalog = catalog;
        this.registry = registry;
        this.workerClient = workerClient;
        this.mergeStrategyRegistry = mergeStrategyRegistry;
    }

    public QueryResult execute(String sql) {
        long startMs = System.currentTimeMillis();
        String queryId = UUID.randomUUID().toString();
        PlannedQuery plan = planner.plan(sql, catalog);

        List<WorkerRegistry.WorkerRecord> workers = registry.listWorkers().stream()
                .sorted(java.util.Comparator.comparing(WorkerRegistry.WorkerRecord::workerId))
                .toList();
        if (workers.isEmpty()) {
            throw new IllegalStateException("No workers registered");
        }

        MergeStrategy mergeStrategy = mergeStrategyRegistry.get(plan.mergeStrategy());
        List<FragmentResult> fragmentResults = new ArrayList<>(plan.fragments().size());
        Map<String, Long> workerDurationsMs = new LinkedHashMap<>();

        for (FragmentSpec fragment : plan.fragments()) {
            WorkerRegistry.WorkerRecord worker = workers.get(fragment.shardId() % workers.size());
            long fragmentStartMs = System.currentTimeMillis();
            List<RowBatchData> batches = workerClient.executeFragment(worker, queryId, fragment);
            long fragmentDurationMs = System.currentTimeMillis() - fragmentStartMs;
            workerDurationsMs.merge(worker.workerId(), fragmentDurationMs, Long::sum);
            fragmentResults.add(new FragmentResult(
                    fragment.fragmentId(),
                    fragment.shardId(),
                    worker.workerId(),
                    fragmentDurationMs,
                    batches));
        }

        long durationMs = System.currentTimeMillis() - startMs;
        MergeContext context = new MergeContext(queryId, plan, fragmentResults, durationMs);
        QueryResult merged = mergeStrategy.merge(context);
        return withTiming(merged, durationMs, workerDurationsMs);
    }

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
