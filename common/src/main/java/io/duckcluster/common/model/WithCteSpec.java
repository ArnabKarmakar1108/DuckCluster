package io.duckcluster.common.model;

import java.util.List;

/** Two-phase plan: shard-local CTE body merge, then coordinator outer query over merged CTE rows. */
public record WithCteSpec(
        QueryAnalysis innerAnalysis,
        List<String> innerColumnNames,
        String outerSql,
        List<String> coordinatorDimensionTables,
        TopKSpec outerTopK) {}
