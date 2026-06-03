package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TopKSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DuckDbMergeSupportNullTest {

    private final GroupByMergeStrategy strategy = new GroupByMergeStrategy();

    @Test
    void mergesEmptyNumericCellsAsNull() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of("category"),
                List.of(new AggregateSpec("total", "__dc_agg_0", AggregateFunction.SUM, "value")),
                List.of("category", "total"));
        PlannedQuery plan = new PlannedQuery(
                "SELECT category, SUM(value) AS total FROM events GROUP BY category",
                List.of("events"),
                List.of(),
                List.of(),
                MergeStrategyType.GROUP_BY_MERGE,
                analysis,
                List.of(),
                TopKSpec.none());
        MergeContext context = new MergeContext(
                "query-null",
                plan,
                List.of(new FragmentResult(
                        0,
                        0,
                        "worker-1",
                        1L,
                        List.of(new RowBatchData(
                                List.of("category", "__dc_agg_0"),
                                List.of(List.of("A", "")))))),
                1L);

        var result = strategy.merge(context);

        assertEquals(1, result.rows().size());
        assertNull(result.rows().get(0).get(1));
    }
}
