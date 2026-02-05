package io.duckcluster.common.model;

public record FragmentSpec(int fragmentId, int shardId, String sql, MergeStrategyType mergeStrategy) {}
