package io.duckcluster.common.planner;

import java.util.Set;

/** Query-scoped broadcast materialization names for fragment SQL generation. */
public record BroadcastScope(Set<String> materializedTableNames, String queryId) {
    public static BroadcastScope of(Set<String> materializedTableNames, String queryId) {
        return new BroadcastScope(materializedTableNames, queryId);
    }

    public boolean isMaterialized(String tableNameLower) {
        return materializedTableNames.contains(tableNameLower);
    }

    public String tempTableName(String tableNameLower) {
        return BroadcastSqlNames.tempTable(queryId, tableNameLower);
    }
}
