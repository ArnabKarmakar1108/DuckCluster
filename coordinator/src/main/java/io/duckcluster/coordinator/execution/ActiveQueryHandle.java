package io.duckcluster.coordinator.execution;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

final class ActiveQueryHandle {
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final CopyOnWriteArrayList<CompletableFuture<?>> fragmentFutures = new CopyOnWriteArrayList<>();
    private volatile Set<String> preparedWorkers = Set.of();

    boolean isCancelled() {
        return cancelled.get();
    }

    void cancel() {
        cancelled.set(true);
        for (CompletableFuture<?> future : fragmentFutures) {
            future.cancel(true);
        }
    }

    void trackFuture(CompletableFuture<?> future) {
        fragmentFutures.add(future);
    }

    void setPreparedWorkers(Set<String> preparedWorkers) {
        this.preparedWorkers = Set.copyOf(preparedWorkers);
    }

    Set<String> preparedWorkers() {
        return preparedWorkers;
    }
}
