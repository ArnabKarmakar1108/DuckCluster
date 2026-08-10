package io.duckcluster.coordinator.execution;

final class AsyncQueryJobEntry {
    final AsyncQueryJob job;
    final long createdAtEpochMs;
    long completedAtEpochMs = -1;
    long retainedBytes;

    AsyncQueryJobEntry(AsyncQueryJob job, long createdAtEpochMs) {
        this.job = job;
        this.createdAtEpochMs = createdAtEpochMs;
    }
}
