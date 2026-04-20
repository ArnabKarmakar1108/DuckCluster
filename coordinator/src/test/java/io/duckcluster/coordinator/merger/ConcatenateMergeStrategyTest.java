package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConcatenateMergeStrategyTest {

    private final ConcatenateMergeStrategy strategy = new ConcatenateMergeStrategy();

    @Test
    void mergesRowsFromAllFragments() {
        PlannedQuery plan = new PlannedQuery(
                "SELECT * FROM events",
                List.of("events"),
                List.of(),
                List.of(),
                MergeStrategyType.CONCATENATE,
                QueryAnalysis.empty(),
                List.of());
        MergeContext context = new MergeContext(
                "query-1",
                plan,
                List.of(
                        new FragmentResult(
                                0,
                                0,
                                "worker-1",
                                5L,
                                List.of(new RowBatchData(
                                        List.of("id", "name"),
                                        List.of(List.of("1", "event-1"), List.of("4", "event-4"))))),
                        new FragmentResult(
                                1,
                                1,
                                "worker-2",
                                7L,
                                List.of(new RowBatchData(
                                        List.of("id", "name"), List.of(List.of("2", "event-2")))))),
                12L);

        QueryResult result = strategy.merge(context);

        assertEquals("query-1", result.queryId());
        assertEquals(List.of("id", "name"), result.columns());
        assertEquals(3, result.rows().size());
        assertEquals(MergeStrategyType.CONCATENATE, result.stats().mergeStrategy());
        assertEquals(2, result.stats().workersUsed());
        assertEquals(12L, result.stats().durationMs());
    }
}
