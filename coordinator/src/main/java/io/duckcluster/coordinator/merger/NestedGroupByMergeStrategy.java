package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.MergeStrategy;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.planner.MergeSqlBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

public final class NestedGroupByMergeStrategy implements MergeStrategy {
    @Override
    public MergeStrategyType type() {
        return MergeStrategyType.NESTED_GROUP_BY_MERGE;
    }

    @Override
    public QueryResult merge(MergeContext context) {
        if (!context.plan().hasNestedDerivedTable()) {
            throw new IllegalStateException("NESTED_GROUP_BY_MERGE requires nested derived table metadata");
        }
        var nested = context.plan().nestedDerivedTable();
        String phase1Sql = MergeSqlBuilder.buildGroupByMerge(
                context.plan().analysis(), io.duckcluster.common.model.TopKSpec.none(), nested.derivedColumnNames());
        String phase2Sql = CoordinatorOuterSqlGenerator.generate(nested);
        return DuckDbMergeSupport.mergeNestedGroupBy(context, stats(context), phase1Sql, phase2Sql, nested);
    }

    private static QueryResult.QueryStats stats(MergeContext context) {
        Map<String, Long> workerDurationsMs = new LinkedHashMap<>();
        for (var fragment : context.fragmentResults()) {
            workerDurationsMs.put(fragment.workerId(), fragment.durationMs());
        }
        return new QueryResult.QueryStats(
                MergeStrategyType.NESTED_GROUP_BY_MERGE,
                workerDurationsMs.size(),
                context.fragmentResults().size(),
                context.durationMs(),
                workerDurationsMs);
    }
}
