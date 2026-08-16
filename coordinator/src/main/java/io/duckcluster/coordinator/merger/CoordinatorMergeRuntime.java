package io.duckcluster.coordinator.merger;

import java.nio.file.Path;

/** Lifecycle hooks for coordinator merge resources. */
public final class CoordinatorMergeRuntime {
    private CoordinatorMergeRuntime() {}

    public static void initialize(int poolSize, long memoryLimitMb, Path tempDirectory) {
        CoordinatorDuckDbPool.initialize(poolSize, memoryLimitMb, tempDirectory);
    }

    public static void shutdown() {
        CoordinatorDuckDbPool.get().close();
    }
}
