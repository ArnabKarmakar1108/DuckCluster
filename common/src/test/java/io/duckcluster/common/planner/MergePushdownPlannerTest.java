package io.duckcluster.common.planner;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.OrderByClause;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TopKSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MergePushdownPlannerTest {

    @Test
    void fragmentTopKOversamplesGroupedLimitByShardCount() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of("l_orderkey"),
                List.of(new AggregateSpec("revenue", "__dc_agg_0", AggregateFunction.SUM, "extendedprice")),
                List.of("l_orderkey", "revenue"));
        TopKSpec coordinatorTopK = new TopKSpec(List.of(new OrderByClause("revenue", true)), 10);

        TopKSpec fragmentTopK = MergePushdownPlanner.fragmentTopK(analysis, coordinatorTopK, 6);

        assertEquals(60, fragmentTopK.limit());
        assertEquals("revenue", fragmentTopK.orderBy().get(0).column());
    }

    @Test
    void fragmentTopKSkipsDistinctCountOrderBy() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of("p_brand"),
                List.of(new AggregateSpec(
                        "cnt",
                        "__dc_distinct_0",
                        AggregateFunction.COUNT,
                        "ps_suppkey",
                        AggregateSpec.AggregatePart.DISTINCT_COUNT)),
                List.of("p_brand", "cnt"));
        TopKSpec coordinatorTopK = new TopKSpec(List.of(new OrderByClause("cnt", true)), 10);

        assertEquals(TopKSpec.none(), MergePushdownPlanner.fragmentTopK(analysis, coordinatorTopK, 6));
    }

    @Test
    void hierarchicalMergeTriggersWhenWorkerHasMultipleFragments() {
        PlannedQuery plan = plannedGroupByQuery();
        MergeContext context = new MergeContext(
                "q-hier",
                plan,
                List.of(
                        fragment(0, "worker-1"),
                        fragment(1, "worker-1"),
                        fragment(2, "worker-2")),
                10L);

        assertTrue(MergePushdownPlanner.useHierarchicalGroupByMerge(context));
    }

    @Test
    void hierarchicalMergeSkipsSingleFragmentPerWorker() {
        PlannedQuery plan = plannedGroupByQuery();
        MergeContext context = new MergeContext(
                "q-flat",
                plan,
                List.of(fragment(0, "worker-1"), fragment(1, "worker-2"), fragment(2, "worker-3")),
                10L);

        assertFalse(MergePushdownPlanner.useHierarchicalGroupByMerge(context));
    }

    @Test
    void hierarchicalMergeSkipsWhenTopKAlreadyCapsRows() {
        PlannedQuery plan = new PlannedQuery(
                "SELECT category, SUM(value) AS total FROM t GROUP BY category ORDER BY total DESC LIMIT 10",
                List.of("t"),
                List.of(),
                List.of(),
                MergeStrategyType.GROUP_BY_MERGE,
                plannedGroupByQuery().analysis(),
                List.of(),
                new TopKSpec(List.of(new OrderByClause("total", true)), 10));
        MergeContext context = new MergeContext(
                "q-topk",
                plan,
                List.of(fragment(0, "worker-1"), fragment(1, "worker-1"), fragment(2, "worker-2")),
                10L);

        assertFalse(MergePushdownPlanner.useHierarchicalGroupByMerge(context));
    }

    private static PlannedQuery plannedGroupByQuery() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of("category"),
                List.of(new AggregateSpec("total", "__dc_agg_0", AggregateFunction.SUM, "value")),
                List.of("category", "total"));
        return new PlannedQuery(
                "SELECT category, SUM(value) AS total FROM t GROUP BY category",
                List.of("t"),
                List.of(),
                List.of(),
                MergeStrategyType.GROUP_BY_MERGE,
                analysis,
                List.of(),
                TopKSpec.none());
    }

    private static FragmentResult fragment(int id, String workerId) {
        return new FragmentResult(
                id,
                id,
                workerId,
                1L,
                List.of(new RowBatchData(List.of("category", "__dc_agg_0"), List.of(List.of("A", "1")))));
    }
}
