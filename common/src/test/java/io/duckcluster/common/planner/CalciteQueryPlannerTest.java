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
        assertTrue(planned.fragments().get(0).sql().contains("(id % 3) = 0"));
        assertTrue(planned.fragments().get(1).sql().contains("(id % 3) = 1"));
        assertTrue(planned.fragments().get(2).sql().contains("(id % 3) = 2"));
    }

    @Test
    void planInjectsShardPredicateIntoExistingWhereClause() {
        PlannedQuery planned = planner.plan("SELECT * FROM events WHERE id > 2", catalog);
        assertTrue(planned.fragments().get(0).sql().contains("WHERE id > 2 AND (id % 3) = 0"));
    }
}
