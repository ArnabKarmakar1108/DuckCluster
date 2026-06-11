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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HierarchicalGroupByMergeTest {

    private final GroupByMergeStrategy strategy = new GroupByMergeStrategy();

    @Test
    void collapsesMultipleFragmentsPerWorkerBeforeFinalMerge() {
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

        List<FragmentResult> fragments = new ArrayList<>();
        fragments.add(fragment(0, "worker-1", "A", "2"));
        fragments.add(fragment(1, "worker-1", "B", "3"));
        fragments.add(fragment(2, "worker-2", "A", "4"));

        MergeContext context = new MergeContext("query-hier", plan, fragments, 5L);
        var result = strategy.merge(context);

        assertEquals(2, result.rows().size());
        assertEquals(6.0, ((Number) result.rows().stream()
                .filter(row -> "A".equals(row.get(0)))
                .findFirst()
                .orElseThrow()
                .get(1)).doubleValue());
        assertEquals(3.0, ((Number) result.rows().stream()
                .filter(row -> "B".equals(row.get(0)))
                .findFirst()
                .orElseThrow()
                .get(1)).doubleValue());
    }

    private static FragmentResult fragment(int id, String workerId, String category, String total) {
        return new FragmentResult(
                id,
                id,
                workerId,
                1L,
                List.of(new RowBatchData(
                        List.of("category", "__dc_agg_0"), List.of(List.of(category, total)))));
    }
}
