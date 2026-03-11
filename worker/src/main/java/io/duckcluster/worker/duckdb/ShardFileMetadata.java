package io.duckcluster.worker.duckdb;

import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record ShardFileMetadata(String tableName, int shardId, Path filePath) {

    private static final Pattern SHARD_FILE_PATTERN =
            Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*)_shard(\\d+)\\.duckdb$");

    public static Optional<ShardFileMetadata> fromPath(Path path) {
        String filename = path.getFileName().toString();
        Matcher matcher = SHARD_FILE_PATTERN.matcher(filename);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        String tableName = matcher.group(1);
        int shardId = Integer.parseInt(matcher.group(2));
        return Optional.of(new ShardFileMetadata(tableName, shardId, path));
    }

    public String catalogName() {
        return tableName + "_shard" + shardId;
    }
}
