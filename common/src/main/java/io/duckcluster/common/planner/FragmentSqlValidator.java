package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.PlannedQuery;

import java.util.Locale;
import java.util.regex.Pattern;

/** Structural checks for worker-executable fragment SQL. */
public final class FragmentSqlValidator {
    private static final Pattern BARE_FROM_TABLE = Pattern.compile("FROM\\s+\"([a-z][a-z0-9_]*)\"",
            Pattern.CASE_INSENSITIVE);

    private FragmentSqlValidator() {}

    public static void validate(String sql, PlannedQuery plan, int shardId, ClusterCatalog catalog) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("Fragment SQL must not be blank");
        }
        String trimmed = sql.strip();
        if (!trimmed.regionMatches(true, 0, "SELECT", 0, 6)) {
            throw new IllegalArgumentException("Fragment SQL must start with SELECT: " + trimmed);
        }
        if (trimmed.endsWith(";")) {
            throw new IllegalArgumentException("Fragment SQL must not end with semicolon");
        }

        for (String tableName : catalog.getShardedTableNames()) {
            if (catalog.table(tableName).shardCount() > 1) {
                String bare = "FROM \"" + tableName.toLowerCase(Locale.ROOT) + "\"";
                if (trimmed.toLowerCase(Locale.ROOT).contains(bare.toLowerCase(Locale.ROOT))) {
                    throw new IllegalArgumentException(
                            "Fragment SQL references unqualified multi-shard table " + tableName + ": " + trimmed);
                }
            }
        }

        String drivingTable = plan.tableName().toLowerCase(Locale.ROOT);
        int expectedShards = catalog.table(drivingTable).shardCount();
        if (plan.fragments().size() != expectedShards) {
            throw new IllegalArgumentException(
                    "Expected " + expectedShards + " fragments for driving table " + drivingTable
                            + " but found " + plan.fragments().size());
        }

        if (expectedShards > 1) {
            String drivingShard = drivingTable + "_shard" + shardId;
            if (!trimmed.contains(drivingShard)) {
                throw new IllegalArgumentException(
                        "Fragment " + shardId + " must reference driving shard " + drivingShard);
            }
        }
    }
}
