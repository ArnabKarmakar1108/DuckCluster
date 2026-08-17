package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;

import java.util.concurrent.ConcurrentHashMap;

/** Per-query coordinator DuckDB staging for streaming fragment batches (4.2). */
public final class MergeStreamRegistry {
    private static final ConcurrentHashMap<String, MergeStreamStaging> ACTIVE = new ConcurrentHashMap<>();

    private MergeStreamRegistry() {}

    public static boolean tryStart(String queryId, PlannedQuery plan) {
        if (plan.mergeStrategy() == MergeStrategyType.CONCATENATE) {
            return false;
        }
        try {
            ACTIVE.put(queryId, MergeStreamStaging.open(queryId, plan));
            return true;
        } catch (IllegalStateException exception) {
            return false;
        }
    }

    public static void appendBatch(String queryId, int fragmentId, RowBatchData batch) {
        MergeStreamStaging staging = ACTIVE.get(queryId);
        if (staging != null) {
            staging.appendBatch(fragmentId, batch);
        }
    }

    static MergeStreamStaging get(String queryId) {
        return ACTIVE.get(queryId);
    }

    public static void remove(String queryId) {
        MergeStreamStaging staging = ACTIVE.remove(queryId);
        if (staging != null) {
            staging.close();
        }
    }
}
