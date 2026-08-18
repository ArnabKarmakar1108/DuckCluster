package io.duckcluster.coordinator.merger;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/** Limits how many coordinator merges may run concurrently (4.5). */
public final class MergeExecutionService implements AutoCloseable {
    private final ExecutorService executor;
    private final Semaphore slots;

    public MergeExecutionService(int maxConcurrentMerges) {
        int size = Math.max(1, maxConcurrentMerges);
        this.slots = new Semaphore(size, true);
        this.executor = Executors.newFixedThreadPool(size, r -> {
            Thread thread = new Thread(r, "query-merge");
            thread.setDaemon(true);
            return thread;
        });
    }

    public <T> T run(Callable<T> mergeWork) {
        try {
            slots.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for merge slot", e);
        }
        try {
            return executor.submit(mergeWork).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during merge", e);
        } finally {
            slots.release();
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
