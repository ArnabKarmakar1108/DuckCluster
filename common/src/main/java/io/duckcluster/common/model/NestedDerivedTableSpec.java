package io.duckcluster.common.model;

import java.util.List;

/** Outer query over a derived table whose inner SELECT has its own GROUP BY. */
public record NestedDerivedTableSpec(
        QueryAnalysis outerAnalysis,
        List<String> derivedColumnNames,
        TopKSpec outerTopK) {}
