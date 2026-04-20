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

class GroupByMergeStrategyTest {

    private final GroupByMergeStrategy strategy = new GroupByMergeStrategy();

    @Test
    void mergesPartialGroupCounts() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of("category"),
                List.of(new AggregateSpec("cnt", "__dc_agg_0", AggregateFunction.COUNT, null)),
                List.of("category", "cnt"));
        PlannedQuery plan = new PlannedQuery(
                "SELECT category, COUNT(*) AS cnt FROM events GROUP BY category",
                List.of("events"),
                List.of(),
                List.of(),
                MergeStrategyType.GROUP_BY_MERGE,
                analysis,
                List.of());
        MergeContext context = new MergeContext(
                "query-1",
                plan,
                List.of(
                        new FragmentResult(
                                0,
                                0,
                                "worker-1",
                                3L,
                                List.of(new RowBatchData(
                                        List.of("category", "__dc_agg_0"),
                                        List.of(List.of("A", "2"), List.of("B", "1"))))),
                        new FragmentResult(
                                1,
                                1,
                                "worker-2",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("category", "__dc_agg_0"), List.of(List.of("C", "1")))))),
                5L);

        QueryResult result = strategy.merge(context);

        assertEquals(List.of("category", "cnt"), result.columns());
        assertEquals(3, result.rows().size());
        assertEquals("2", result.rows().stream()
                .filter(row -> "A".equals(row.get(0)))
                .findFirst()
                .orElseThrow()
                .get(1)
                .toString());
    }
}
