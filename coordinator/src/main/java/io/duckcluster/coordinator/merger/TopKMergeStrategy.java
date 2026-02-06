package io.duckcluster.coordinator.merger;

import io.duckcluster.common.model.MergeStrategyType;

public final class TopKMergeStrategy extends UnsupportedMergeStrategy {
    public TopKMergeStrategy() {
        super(MergeStrategyType.TOP_K);
    }
}
