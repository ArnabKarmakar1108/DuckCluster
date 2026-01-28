package io.duckcluster.common.merger;

import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryResult;

public interface MergeStrategy {
    MergeStrategyType type();

    QueryResult merge(MergeContext context);
}
