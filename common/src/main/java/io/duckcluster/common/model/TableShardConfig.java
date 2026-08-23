package io.duckcluster.common.model;

/**
 * Sharding layout for a table. A declared {@code shardKey} is a contract that row {@code r} lives in
 * shard {@code abs(hash(r.shardKey)) % shardCount} (DuckDB {@code hash}); the planner relies on it for
 * co-partitioned and shuffle joins. Tables discovered only from worker registration are undeclared.
 */
public record TableShardConfig(String tableName, int shardCount, String shardKey) {
    public static final String UNDECLARED_SHARD_KEY = "rowid";

    public TableShardConfig {
        tableName = tableName.toLowerCase();
        if (shardCount < 1) {
            throw new IllegalArgumentException("shardCount must be >= 1");
        }
        if (shardKey == null || shardKey.isBlank()) {
            shardKey = UNDECLARED_SHARD_KEY;
        }
    }

    public boolean hasDeclaredShardKey() {
        return !UNDECLARED_SHARD_KEY.equalsIgnoreCase(shardKey);
    }
}
