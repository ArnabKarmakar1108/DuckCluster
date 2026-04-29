package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.OrderByClause;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.model.TopKSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TopKMergeStrategyTest {

    private final TopKMergeStrategy strategy = new TopKMergeStrategy();

    @Test
    void mergesTopRowsAcrossFragments() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of(),
                List.of(),
                List.of("id", "value"));
        PlannedQuery plan = new PlannedQuery(
                "SELECT id, value FROM events ORDER BY value DESC LIMIT 2",
                List.of("events"),
                List.of(),
                List.of(),
                MergeStrategyType.TOP_K,
                analysis,
                List.of(),
                new TopKSpec(List.of(new OrderByClause("value", true)), 2));
        MergeContext context = new MergeContext(
                "query-1",
                plan,
                List.of(
                        new FragmentResult(
                                0,
                                0,
                                "worker-1",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("id", "value"),
                                        List.of(List.of("1", "50"), List.of("2", "30"))))),
                        new FragmentResult(
                                1,
                                1,
                                "worker-2",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("id", "value"),
                                        List.of(List.of("3", "40"), List.of("4", "10")))))),
                4L);

        QueryResult result = strategy.merge(context);

        assertEquals(List.of("id", "value"), result.columns());
        assertEquals(2, result.rows().size());
        assertEquals("1", result.rows().get(0).get(0).toString());
        assertEquals("50", result.rows().get(0).get(1).toString());
        assertEquals("3", result.rows().get(1).get(0).toString());
        assertEquals("40", result.rows().get(1).get(1).toString());
    }
}
