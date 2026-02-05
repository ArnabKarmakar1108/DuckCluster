package io.duckcluster.common.model;

import java.util.List;

public record PlannedQuery(
        String originalSql,
        String tableName,
        List<FragmentSpec> fragments,
        MergeStrategyType mergeStrategy,
        List<ColumnDef> schema) {}
