package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.MergeStrategy;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.planner.MergeSqlBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

public final class GroupByMergeStrategy implements MergeStrategy {
    @Override
    public MergeStrategyType type() {
        return MergeStrategyType.GROUP_BY_MERGE;
    }

    @Override
    public QueryResult merge(MergeContext context) {
        String mergeSql = MergeSqlBuilder.buildGroupByMerge(context.plan().analysis());
        return DuckDbMergeSupport.mergeWithSql(context, stats(context), mergeSql);
    }

    private static QueryResult.QueryStats stats(MergeContext context) {
        Map<String, Long> workerDurationsMs = new LinkedHashMap<>();
        for (var fragment : context.fragmentResults()) {
            workerDurationsMs.put(fragment.workerId(), fragment.durationMs());
        }
        return new QueryResult.QueryStats(
                MergeStrategyType.GROUP_BY_MERGE,
                workerDurationsMs.size(),
                context.fragmentResults().size(),
                context.durationMs(),
                workerDurationsMs);
    }
}
