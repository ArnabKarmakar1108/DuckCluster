package io.duckcluster.common.merger;

import io.duckcluster.common.model.PlannedQuery;

import java.util.List;
import java.util.Map;

public record MergeContext(
        String queryId,
        PlannedQuery plan,
        List<FragmentResult> fragmentResults,
        long durationMs,
        Map<String, RowBatchData> coordinatorTables) {

    public MergeContext(String queryId, PlannedQuery plan, List<FragmentResult> fragmentResults, long durationMs) {
        this(queryId, plan, fragmentResults, durationMs, Map.of());
    }
}
