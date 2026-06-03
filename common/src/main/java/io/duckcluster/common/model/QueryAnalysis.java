package io.duckcluster.common.model;

import java.util.List;

public record QueryAnalysis(
        List<String> groupByColumns,
        List<AggregateSpec> aggregates,
        List<String> outputColumnNames,
        List<ComputedOutputSpec> computedOutputs) {

    public QueryAnalysis(List<String> groupByColumns, List<AggregateSpec> aggregates, List<String> outputColumnNames) {
        this(groupByColumns, aggregates, outputColumnNames, List.of());
    }

    public static QueryAnalysis empty() {
        return new QueryAnalysis(List.of(), List.of(), List.of(), List.of());
    }

    public boolean hasAggregates() {
        return !aggregates.isEmpty();
    }

    public boolean hasComputedOutputs() {
        return !computedOutputs.isEmpty();
    }

    public boolean hasDistinctCountAggregates() {
        return aggregates.stream().anyMatch(a -> a.part() == AggregateSpec.AggregatePart.DISTINCT_COUNT);
    }
}
