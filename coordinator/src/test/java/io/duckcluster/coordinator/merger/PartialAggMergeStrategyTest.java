package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.QueryResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartialAggMergeStrategyTest {

    private final PartialAggMergeStrategy strategy = new PartialAggMergeStrategy();

    @Test
    void mergesPartialCountsAndSums() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of(),
                List.of(
                        new AggregateSpec("count", "__dc_agg_0", AggregateFunction.COUNT, null),
                        new AggregateSpec("total", "__dc_agg_1", AggregateFunction.SUM, "id")),
                List.of("count", "total"));
        PlannedQuery plan = new PlannedQuery(
                "SELECT COUNT(*), SUM(id) FROM events",
                "events",
                List.of(),
                MergeStrategyType.PARTIAL_AGG,
                analysis,
                List.of());
        MergeContext context = new MergeContext(
                "query-2",
                plan,
                List.of(
                        new FragmentResult(
                                0,
                                0,
                                "worker-1",
                                4L,
                                List.of(new RowBatchData(
                                        List.of("__dc_agg_0", "__dc_agg_1"), List.of(List.of("2", "9"))))),
                        new FragmentResult(
                                1,
                                1,
                                "worker-2",
                                3L,
                                List.of(new RowBatchData(
                                        List.of("__dc_agg_0", "__dc_agg_1"), List.of(List.of("1", "4")))))),
                7L);

        QueryResult result = strategy.merge(context);

        assertEquals(List.of("count", "total"), result.columns());
        assertEquals(1, result.rows().size());
        assertEquals("3", result.rows().get(0).get(0).toString());
        assertEquals("13", result.rows().get(0).get(1).toString());
    }
}
