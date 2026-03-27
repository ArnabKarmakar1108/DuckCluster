package io.duckcluster.common.model;

public record AggregateSpec(
        String outputName,
        String mergeColumnName,
        AggregateFunction function,
        String inputColumn) {}
