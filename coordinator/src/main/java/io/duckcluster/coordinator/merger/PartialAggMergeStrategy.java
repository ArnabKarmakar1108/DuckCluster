package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.MergeStrategy;
import io.duckcluster.common.model.MergeStrategyType;

public final class PartialAggMergeStrategy extends UnsupportedMergeStrategy {
    public PartialAggMergeStrategy() {
        super(MergeStrategyType.PARTIAL_AGG);
    }
}
