package io.duckcluster.common.model;

public enum MergeStrategyType {
    CONCATENATE,
    PARTIAL_AGG,
    GROUP_BY_MERGE,
    NESTED_GROUP_BY_MERGE,
    WITH_CTE_MERGE,
    TOP_K
}
