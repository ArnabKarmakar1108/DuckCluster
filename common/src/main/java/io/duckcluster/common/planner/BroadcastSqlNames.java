package io.duckcluster.common.planner;

/** Worker-side temp-table names shared by the planner (fragment SQL) and workers (materialization). */
public final class BroadcastSqlNames {
    public static final String PREFIX = "__dc_bcast_";
    public static final String SHUFFLE_PREFIX = "__dc_shuf_";

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

    /** Broadcast table shared by all queries reading the same active data version. */
    public static String versionedTable(long dataVersion, String tableName) {
        return PREFIX + "v" + dataVersion + "_" + tableName.toLowerCase();
    }

    public static String broadcastTable(String queryId, String tableName, long activeDataVersion) {
        return activeDataVersion > 0 ? versionedTable(activeDataVersion, tableName) : tempTable(queryId, tableName);
    }

    public static String shuffleBucketTable(String queryId, String tableName, int bucket) {
        return SHUFFLE_PREFIX + queryId + "_" + tableName.toLowerCase() + "_b" + bucket;
    }

    public static String shuffleQueryPrefix(String queryId) {
        return SHUFFLE_PREFIX + queryId + "_";
    }
}
