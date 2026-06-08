package io.duckcluster.common.planner;

/** Stable temp-table names for worker-side broadcast materialization. */
public final class BroadcastSqlNames {
    public static final String PREFIX = "__dc_bcast_";

    private BroadcastSqlNames() {}

    public static String tempTable(String tableName) {
        return PREFIX + tableName.toLowerCase();
    }
}
