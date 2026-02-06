package io.duckcluster.coordinator.merger;

import io.duckcluster.common.model.MergeStrategyType;

public final class GroupByMergeStrategy extends UnsupportedMergeStrategy {
    public GroupByMergeStrategy() {
        super(MergeStrategyType.GROUP_BY_MERGE);
    }
}
