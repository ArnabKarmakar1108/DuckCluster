package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.MergeStrategy;
import io.duckcluster.common.model.PlannedQuery;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CalciteQueryPlannerTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();

    @Test
    void detectsConcatenateStrategyForSimpleSelect() {
        MergeStrategy strategy = planner.detectMergeStrategy(planner.parse("SELECT * FROM events"));
        assertEquals(MergeStrategy.CONCATENATE, strategy);
    }

    @Test
    void detectsGroupByMergeStrategy() {
        MergeStrategy strategy = planner.detectMergeStrategy(
                planner.parse("SELECT category, COUNT(*) FROM events GROUP BY category"));
        assertEquals(MergeStrategy.GROUP_BY_MERGE, strategy);
    }

    @Test
    void planReturnsEmptyFragmentsInPhaseZero() {
        PlannedQuery planned = planner.plan("SELECT * FROM events", ClusterCatalog.empty());
        assertEquals(MergeStrategy.CONCATENATE, planned.mergeStrategy());
        assertEquals(0, planned.fragments().size());
    }
}
