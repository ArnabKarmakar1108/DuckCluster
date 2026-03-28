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
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.merger.MergeStrategyRegistry;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public final class QueryExecutionService {
    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutionService.class);

    private final QueryPlanner planner;
    private final ClusterCatalog catalog;
    private final WorkerRegistry registry;
    private final WorkerNodeClient workerClient;
    private final MergeStrategyRegistry mergeStrategyRegistry;
    private final ShardCatalog shardCatalog;

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
    }

    public QueryResult execute(String sql) {
        long startMs = System.currentTimeMillis();
        String queryId = UUID.randomUUID().toString();
        PlannedQuery plan = planner.plan(sql, catalog);

        if (registry.workerCount() == 0) {
            throw new IllegalStateException("No workers registered");
        }

        MergeStrategy mergeStrategy = mergeStrategyRegistry.get(plan.mergeStrategy());
        List<FragmentResult> fragmentResults = new ArrayList<>(plan.fragments().size());
        Map<String, Long> workerDurationsMs = new LinkedHashMap<>();

        for (FragmentSpec fragment : plan.fragments()) {
            List<String> owners = shardCatalog.getOwners(plan.tableName(), fragment.shardId());
            ExecutionOutcome outcome = executeWithFallback(queryId, fragment, owners);
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

    private ExecutionOutcome executeWithFallback(
            String queryId, FragmentSpec fragment, List<String> owners) {
        for (int i = 0; i < owners.size(); i++) {
            String workerId = owners.get(i);
            Optional<WorkerRegistry.WorkerRecord> workerOpt = registry.getWorker(workerId);
            if (workerOpt.isEmpty()) {
                LOG.warn("Shard {} owner {} not in registry, trying next", fragment.shardId(), workerId);
                continue;
            }
            WorkerRegistry.WorkerRecord worker = workerOpt.get();
            String schedulingOutcome = i == 0 ? "LOCAL" : "REPLICA";
            try {
                long fragmentStartMs = System.currentTimeMillis();
                List<RowBatchData> batches = workerClient.executeFragment(worker, queryId, fragment);
                long fragmentDurationMs = System.currentTimeMillis() - fragmentStartMs;
                LOG.debug("Fragment {} executed on {} ({})", fragment.fragmentId(), workerId, schedulingOutcome);
                return new ExecutionOutcome(workerId, fragmentDurationMs, batches);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.RESOURCE_EXHAUSTED) {
                    LOG.warn("Worker {} pool exhausted for fragment {}, trying next owner",
                            workerId, fragment.fragmentId());
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException(
                "All owners exhausted for shard " + fragment.shardId() + ": " + owners);
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
