package io.duckcluster.coordinator.execution;

final class AsyncQueryJobEntry {
    final AsyncQueryJob job;
    final QueryResultSpool spool;
    final long createdAtEpochMs;
    long completedAtEpochMs = -1;
    long retainedBytes;

    AsyncQueryJobEntry(AsyncQueryJob job, QueryResultSpool spool, long createdAtEpochMs) {
        this.job = job;
        this.spool = spool;
        this.createdAtEpochMs = createdAtEpochMs;
    }
}
