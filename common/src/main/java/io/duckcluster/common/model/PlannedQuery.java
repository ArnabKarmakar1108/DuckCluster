package io.duckcluster.common.model;

import java.util.List;

public record PlannedQuery(
        String originalSql,
        List<String> shardedTables,
        List<BroadcastTable> broadcastTables,
        List<FragmentSpec> fragments,
        MergeStrategyType mergeStrategy,
        QueryAnalysis analysis,
        List<ColumnDef> schema,
        TopKSpec topK,
        NestedDerivedTableSpec nestedDerivedTable) {

    public PlannedQuery(
            String originalSql,
            List<String> shardedTables,
            List<BroadcastTable> broadcastTables,
            List<FragmentSpec> fragments,
            MergeStrategyType mergeStrategy,
            QueryAnalysis analysis,
            List<ColumnDef> schema,
            TopKSpec topK) {
        this(originalSql, shardedTables, broadcastTables, fragments, mergeStrategy,
                analysis, schema, topK, null);
    }

    public String tableName() {
        return shardedTables.get(0);
    }

    public boolean hasNestedDerivedTable() {
        return nestedDerivedTable != null;
    }
}
