package io.duckcluster.coordinator.execution;

import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryResultSizeEstimatorTest {

    @Test
    void estimateGrowsWithRowPayload() {
        QueryResult small = new QueryResult(
                "q",
                List.of("a"),
                List.of(List.of(1)),
                new QueryResult.QueryStats(MergeStrategyType.CONCATENATE, 1, 1, 1, Map.of()));
        QueryResult large = new QueryResult(
                "q",
                List.of("a"),
                List.of(List.of("x".repeat(1000))),
                new QueryResult.QueryStats(MergeStrategyType.CONCATENATE, 1, 1, 1, Map.of()));

        long smallBytes = QueryResultSizeEstimator.estimateBytes(small);
        long largeBytes = QueryResultSizeEstimator.estimateBytes(large);
        assertTrue(largeBytes > smallBytes + 500);
    }
}
