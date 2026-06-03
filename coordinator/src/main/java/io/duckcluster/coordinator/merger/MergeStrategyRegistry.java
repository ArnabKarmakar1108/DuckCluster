package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.MergeStrategy;
import io.duckcluster.common.model.MergeStrategyType;

import java.util.EnumMap;
import java.util.Map;

public final class MergeStrategyRegistry {
    private final Map<MergeStrategyType, MergeStrategy> strategies;

    public MergeStrategyRegistry() {
        strategies = new EnumMap<>(MergeStrategyType.class);
        register(new ConcatenateMergeStrategy());
        register(new PartialAggMergeStrategy());
        register(new GroupByMergeStrategy());
        register(new NestedGroupByMergeStrategy());
        register(new WithCteMergeStrategy());
        register(new TopKMergeStrategy());
    }

    public MergeStrategy get(MergeStrategyType type) {
        MergeStrategy strategy = strategies.get(type);
        if (strategy == null) {
            throw new IllegalArgumentException("No merge strategy registered for " + type);
        }
        return strategy;
    }

    private void register(MergeStrategy strategy) {
        strategies.put(strategy.type(), strategy);
    }
}
