package io.duckcluster.common.planner;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.OrderByClause;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TopKSpec;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Decides when merge work can be pushed to fragments or collapsed hierarchically. */
public final class MergePushdownPlanner {
    private MergePushdownPlanner() {}

    /**
     * For GROUP BY + ORDER BY LIMIT queries, each fragment returns at most {@code limit × shardCount}
     * locally ranked rows. Global merge still applies final ORDER BY LIMIT.
     */
    public static TopKSpec fragmentTopK(QueryAnalysis analysis, TopKSpec coordinatorTopK, int shardCount) {
        if (analysis.groupByColumns().isEmpty()) {
            return coordinatorTopK;
        }
        if (!coordinatorTopK.hasLimit() || !coordinatorTopK.hasOrderBy()) {
            return TopKSpec.none();
        }
        if (!topKOversampleSafe(analysis, coordinatorTopK)) {
            return TopKSpec.none();
        }
        int oversampleLimit = coordinatorTopK.limit() * Math.max(shardCount, 1);
        return new TopKSpec(coordinatorTopK.orderBy(), oversampleLimit);
    }

    public static boolean topKOversampleSafe(QueryAnalysis analysis, TopKSpec topK) {
        for (OrderByClause clause : topK.orderBy()) {
            if (analysis.groupByColumns().contains(clause.column())) {
                continue;
            }
            AggregateSpec aggregate = aggregateForOrderColumn(analysis, clause.column());
            if (aggregate == null) {
                return false;
            }
            if (aggregate.part() == AggregateSpec.AggregatePart.DISTINCT_COUNT) {
                return false;
            }
            if (aggregate.function() != AggregateFunction.SUM && aggregate.function() != AggregateFunction.COUNT) {
                return false;
            }
        }
        return true;
    }

    public static boolean useHierarchicalGroupByMerge(MergeContext context) {
        if (context.plan().mergeStrategy() != MergeStrategyType.GROUP_BY_MERGE) {
            return false;
        }
        TopKSpec topK = context.plan().topK();
        if (topK.hasLimit() && topK.hasOrderBy()) {
            return false;
        }
        if (context.fragmentResults().size() <= 1) {
            return false;
        }
        Map<String, Long> fragmentsPerWorker = context.fragmentResults().stream()
                .collect(Collectors.groupingBy(FragmentResult::workerId, Collectors.counting()));
        return fragmentsPerWorker.values().stream().anyMatch(count -> count > 1);
    }

    private static AggregateSpec aggregateForOrderColumn(QueryAnalysis analysis, String orderColumn) {
        for (AggregateSpec aggregate : analysis.aggregates()) {
            if (aggregate.part() == AggregateSpec.AggregatePart.AVG_COUNT) {
                continue;
            }
            if (aggregate.outputName().equals(orderColumn)) {
                return aggregate;
            }
        }
        return null;
    }
}
