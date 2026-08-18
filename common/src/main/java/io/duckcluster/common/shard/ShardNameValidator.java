package io.duckcluster.common.shard;

import java.nio.file.Path;

/** Validates shard table identifiers to block path traversal in transfer RPCs. */
public final class ShardNameValidator {
    private static final String TABLE_PATTERN = "[A-Za-z0-9_]+";

    private ShardNameValidator() {}

    public static void validateTableName(String tableName) {
        if (tableName == null || tableName.isBlank() || !tableName.matches(TABLE_PATTERN)) {
            throw new IllegalArgumentException("invalid table name: " + tableName);
        }
    }

    public static void validateShardId(int shardId) {
        if (shardId < 0) {
            throw new IllegalArgumentException("invalid shard id: " + shardId);
        }
    }

    public static void validatePathUnderBase(Path baseDir, Path resolvedPath) {
        Path base = baseDir.toAbsolutePath().normalize();
        Path target = resolvedPath.toAbsolutePath().normalize();
        if (!target.startsWith(base)) {
            throw new IllegalArgumentException("path escapes data directory: " + resolvedPath);
        }
    }
}
