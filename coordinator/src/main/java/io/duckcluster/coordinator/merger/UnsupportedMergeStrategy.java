package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.MergeStrategy;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryResult;

abstract class UnsupportedMergeStrategy implements MergeStrategy {
    private final MergeStrategyType type;

    UnsupportedMergeStrategy(MergeStrategyType type) {
        this.type = type;
    }

    @Override
    public MergeStrategyType type() {
        return type;
    }

    @Override
    public QueryResult merge(MergeContext context) {
        throw new UnsupportedOperationException(type() + " merge is not implemented yet");
    }
}
