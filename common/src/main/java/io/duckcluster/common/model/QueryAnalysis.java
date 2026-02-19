package io.duckcluster.common.model;

import java.util.List;

public record QueryAnalysis(
        List<String> groupByColumns, List<AggregateSpec> aggregates, List<String> outputColumnNames) {

    public static QueryAnalysis empty() {
        return new QueryAnalysis(List.of(), List.of(), List.of());
    }

    public boolean hasAggregates() {
        return !aggregates.isEmpty();
    }
}
