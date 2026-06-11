package io.duckcluster.coordinator.merger;

/** Lifecycle hooks for coordinator merge resources. */
public final class CoordinatorMergeRuntime {
    private CoordinatorMergeRuntime() {}

    public static void shutdown() {
        CoordinatorDuckDbPool.get().close();
    }
}
