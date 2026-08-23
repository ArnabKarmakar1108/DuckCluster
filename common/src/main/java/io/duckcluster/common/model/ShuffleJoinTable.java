package io.duckcluster.common.model;

/**
 * A non-driving table that is hash-repartitioned to line up with the driving table's shards.
 * Row {@code r} goes to bucket {@code abs(hash(CAST(r.joinColumn AS joinKeyType))) % bucketCount},
 * the same function that placed driving rows, so bucket {@code i} only needs driving shard {@code i}.
 */
public record ShuffleJoinTable(
        String tableName,
        int sourceShardCount,
        String joinColumn,
        String joinKeyType,
        int bucketCount) {}
