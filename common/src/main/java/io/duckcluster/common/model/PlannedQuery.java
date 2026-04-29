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
        TopKSpec topK) {

    public String tableName() {
        return shardedTables.get(0);
    }
}
