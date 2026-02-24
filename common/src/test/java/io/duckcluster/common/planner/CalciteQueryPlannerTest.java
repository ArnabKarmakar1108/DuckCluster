package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CalciteQueryPlannerTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final ClusterCatalog catalog = ClusterCatalog.demo(3);

    @Test
    void detectsConcatenateStrategyForSimpleSelect() {
        MergeStrategyType strategy = planner.detectMergeStrategy(planner.parse("SELECT * FROM events"));
        assertEquals(MergeStrategyType.CONCATENATE, strategy);
    }

    @Test
    void detectsGroupByMergeStrategy() {
        MergeStrategyType strategy = planner.detectMergeStrategy(
                planner.parse("SELECT category, COUNT(*) FROM events GROUP BY category"));
        assertEquals(MergeStrategyType.GROUP_BY_MERGE, strategy);
    }

    @Test
    void planGeneratesOneFragmentPerShard() {
        PlannedQuery planned = planner.plan("SELECT * FROM events", catalog);
        assertEquals(MergeStrategyType.CONCATENATE, planned.mergeStrategy());
        assertEquals(3, planned.fragments().size());
        assertTrue(planned.fragments().get(0).sql().contains("= 0"));
        assertTrue(planned.fragments().get(1).sql().contains("= 1"));
        assertTrue(planned.fragments().get(2).sql().contains("= 2"));
    }

    @Test
    void planInjectsShardPredicateIntoExistingWhereClause() {
        PlannedQuery planned = planner.plan("SELECT * FROM events WHERE id > 2", catalog);
        assertTrue(planned.fragments().get(0).sql().contains("> 2"));
        assertTrue(planned.fragments().get(0).sql().contains("MOD"));
        assertTrue(planned.fragments().get(0).sql().contains("= 0"));
    }

    @Test
    void groupByFragmentsKeepAggregatePushdown() {
        PlannedQuery planned = planner.plan("SELECT category, COUNT(*) AS cnt FROM events GROUP BY category", catalog);
        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertEquals(1, planned.analysis().aggregates().size());
        assertTrue(planned.fragments().get(0).sql().contains("GROUP BY"));
        assertTrue(planned.fragments().get(0).sql().contains("__dc_agg_0"));
    }

    @Test
    void partialAggFragmentsIncludeAggregateAliases() {
        PlannedQuery planned = planner.plan("SELECT COUNT(*), SUM(id) FROM events", catalog);
        assertEquals(MergeStrategyType.PARTIAL_AGG, planned.mergeStrategy());
        assertTrue(planned.fragments().get(1).sql().contains("__dc_agg_0"));
        assertTrue(planned.fragments().get(1).sql().contains("__dc_agg_1"));
    }
}
