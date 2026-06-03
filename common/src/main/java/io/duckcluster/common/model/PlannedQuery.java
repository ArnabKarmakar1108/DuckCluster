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
        NestedDerivedTableSpec nestedDerivedTable,
        WithCteSpec withCte,
        List<BroadcastTable> subqueryBroadcastTables,
        List<String> correlatedCoPartitionTables) {

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
                analysis, schema, topK, null, null, List.of(), List.of());
    }

    public PlannedQuery(
            String originalSql,
            List<String> shardedTables,
            List<BroadcastTable> broadcastTables,
            List<FragmentSpec> fragments,
            MergeStrategyType mergeStrategy,
            QueryAnalysis analysis,
            List<ColumnDef> schema,
            TopKSpec topK,
            NestedDerivedTableSpec nestedDerivedTable) {
        this(originalSql, shardedTables, broadcastTables, fragments, mergeStrategy,
                analysis, schema, topK, nestedDerivedTable, null, List.of(), List.of());
    }

    public PlannedQuery(
            String originalSql,
            List<String> shardedTables,
            List<BroadcastTable> broadcastTables,
            List<FragmentSpec> fragments,
            MergeStrategyType mergeStrategy,
            QueryAnalysis analysis,
            List<ColumnDef> schema,
            TopKSpec topK,
            NestedDerivedTableSpec nestedDerivedTable,
            List<BroadcastTable> subqueryBroadcastTables) {
        this(originalSql, shardedTables, broadcastTables, fragments, mergeStrategy,
                analysis, schema, topK, nestedDerivedTable, null, subqueryBroadcastTables, List.of());
    }

    public String tableName() {
        return shardedTables.get(0);
    }

    public boolean hasNestedDerivedTable() {
        return nestedDerivedTable != null;
    }

    public boolean hasWithCte() {
        return withCte != null;
    }
}
