package io.duckcluster.coordinator.execution;

import io.duckcluster.common.model.QueryResult;

import java.util.Optional;

public final class AsyncQueryJob {
    public enum Status {
        QUEUED,
        RUNNING,
        SUCCEEDED,
        FAILED,
        EXPIRED
    }

    private volatile Status status = Status.QUEUED;
    private volatile QueryResult result;
    private volatile String errorMessage;

    public Status status() {
        return status;
    }

    public Optional<QueryResult> result() {
        return Optional.ofNullable(result);
    }

    public Optional<String> errorMessage() {
        return Optional.ofNullable(errorMessage);
    }

    void markRunning() {
        status = Status.RUNNING;
    }

    void complete(QueryResult queryResult) {
        result = queryResult;
        status = Status.SUCCEEDED;
    }

    void fail(String message) {
        errorMessage = message != null ? message : "query failed";
        status = Status.FAILED;
    }

    void expire() {
        result = null;
        errorMessage = null;
        status = Status.EXPIRED;
    }
}
