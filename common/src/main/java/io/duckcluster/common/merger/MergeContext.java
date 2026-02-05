package io.duckcluster.common.merger;

import io.duckcluster.common.model.PlannedQuery;

import java.util.List;

public record MergeContext(
        String queryId, PlannedQuery plan, List<FragmentResult> fragmentResults, long durationMs) {}
