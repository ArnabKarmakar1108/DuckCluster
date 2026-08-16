package io.duckcluster.worker.duckdb;

import io.duckcluster.proto.v1.ColumnSchema;
import io.duckcluster.proto.v1.ShardOwnership;

import java.util.List;

public final class ShardOwnershipReporter {
    private ShardOwnershipReporter() {}

    public static List<ShardOwnership> toOwnership(ShardManager shardManager, List<ShardFileMetadata> shards) {
        return shards.stream().map(meta -> toOwnership(shardManager, meta)).toList();
    }

    public static ShardOwnership toOwnership(ShardManager shardManager, ShardFileMetadata meta) {
        ShardOwnership.Builder builder = ShardOwnership.newBuilder()
                .setTableName(meta.tableName())
                .setShardId(meta.shardId())
                .setRowCount(shardManager.estimateRowCount(meta));
        if (meta.shardId() == 0) {
            for (ShardManager.ShardColumnMetadata column : shardManager.describeTable(meta)) {
                builder.addColumns(ColumnSchema.newBuilder()
                        .setName(column.name())
                        .setType(column.type())
                        .build());
            }
        }
        return builder.build();
    }
}
