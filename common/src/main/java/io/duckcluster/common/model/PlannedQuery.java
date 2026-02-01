package io.duckcluster.common.model;

import java.util.List;

public record PlannedQuery(
        String originalSql,
        List<FragmentSpec> fragments,
        MergeStrategy mergeStrategy,
        List<ColumnDef> schema) {}
