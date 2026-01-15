package io.duckcluster.common.model;

public record TableShardConfig(String tableName, int shardCount, String shardKey) {
    public TableShardConfig {
        tableName = tableName.toLowerCase();
        if (shardCount < 1) {
            throw new IllegalArgumentException("shardCount must be >= 1");
        }
        if (shardKey == null || shardKey.isBlank()) {
            shardKey = "rowid";
        }
    }
}
