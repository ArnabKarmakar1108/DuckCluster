package io.duckcluster.common.planner;

/** Stable temp-table names for worker-side broadcast materialization. */
public final class BroadcastSqlNames {
    public static final String PREFIX = "__dc_bcast_";

    private BroadcastSqlNames() {}

    /** Legacy name without query scope (tests and single-query local runs). */
    public static String tempTable(String tableName) {
        return tempTable(null, tableName);
    }

    public static String tempTable(String queryId, String tableName) {
        String table = tableName.toLowerCase();
        if (queryId == null || queryId.isBlank()) {
            return PREFIX + table;
        }
        return PREFIX + queryId + "_" + table;
    }
}
