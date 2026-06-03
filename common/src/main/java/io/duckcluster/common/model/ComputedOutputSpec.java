package io.duckcluster.common.model;

/** Merge-time SQL expression for a SELECT item built from nested partial aggregates. */
public record ComputedOutputSpec(String outputName, String mergeExpressionSql) {}
