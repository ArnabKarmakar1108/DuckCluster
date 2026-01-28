package io.duckcluster.common.model;

import java.util.List;
import java.util.Map;

public record QueryResult(
        String queryId,
        List<String> columns,
        List<List<Object>> rows,
        QueryStats stats) {

    public record QueryStats(
            MergeStrategyType mergeStrategy,
            int workersUsed,
            int fragmentsExecuted,
            long durationMs,
            Map<String, Long> workerDurationsMs) {}
}
