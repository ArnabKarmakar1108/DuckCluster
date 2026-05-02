package io.duckcluster.coordinator.execution;

import io.duckcluster.common.registry.WorkerRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class WorkerFragmentTracker {
    private final WorkerRegistry registry;
    private final Map<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();

    WorkerFragmentTracker(WorkerRegistry registry) {
        this.registry = registry;
    }

    int inFlightCount(String workerId) {
        AtomicInteger count = inFlight.get(workerId);
        return count == null ? 0 : count.get();
    }

    boolean hasCapacity(String workerId) {
        return registry.getWorker(workerId)
                .map(worker -> inFlightCount(workerId) < worker.numThreads())
                .orElse(false);
    }

    void acquireBlocking(String workerId, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (tryAcquire(workerId)) {
                return;
            }
            sleepQuietly(50);
        }
        throw new IllegalStateException(
                "Timed out waiting for worker " + workerId + " to accept a fragment");
    }

    void release(String workerId) {
        AtomicInteger count = inFlight.get(workerId);
        if (count != null) {
            count.updateAndGet(value -> Math.max(0, value - 1));
        }
    }

    private boolean tryAcquire(String workerId) {
        WorkerRegistry.WorkerRecord worker = registry.getWorker(workerId).orElse(null);
        if (worker == null) {
            return false;
        }
        AtomicInteger count = inFlight.computeIfAbsent(workerId, ignored -> new AtomicInteger(0));
        synchronized (count) {
            if (count.get() >= worker.numThreads()) {
                return false;
            }
            count.incrementAndGet();
            return true;
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for worker capacity", e);
        }
    }
}
