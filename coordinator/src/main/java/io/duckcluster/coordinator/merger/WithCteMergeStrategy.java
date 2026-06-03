package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.MergeStrategy;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.planner.MergeSqlBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

public final class WithCteMergeStrategy implements MergeStrategy {
    @Override
    public MergeStrategyType type() {
        return MergeStrategyType.WITH_CTE_MERGE;
    }

    @Override
    public QueryResult merge(MergeContext context) {
        if (!context.plan().hasWithCte()) {
            throw new IllegalStateException("WITH_CTE_MERGE requires withCte metadata");
        }
        var withCte = context.plan().withCte();
        String phase1Sql = MergeSqlBuilder.buildGroupByMerge(
                context.plan().analysis(), io.duckcluster.common.model.TopKSpec.none(), withCte.innerColumnNames());
        return DuckDbMergeSupport.mergeWithCte(
                context, stats(context), phase1Sql, withCte.outerSql(), context.coordinatorTables());
    }

    private static QueryResult.QueryStats stats(MergeContext context) {
        Map<String, Long> workerDurationsMs = new LinkedHashMap<>();
        for (var fragment : context.fragmentResults()) {
            workerDurationsMs.put(fragment.workerId(), fragment.durationMs());
        }
        return new QueryResult.QueryStats(
                MergeStrategyType.WITH_CTE_MERGE,
                workerDurationsMs.size(),
                context.fragmentResults().size(),
                context.durationMs(),
                workerDurationsMs);
    }
}
