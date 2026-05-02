package io.duckcluster.coordinator.execution;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public final class FragmentExecutionException extends RuntimeException {
    private final Status status;

    public FragmentExecutionException(Status status, StatusRuntimeException cause) {
        super(cause.getMessage(), cause);
        this.status = status;
    }

    public Status status() {
        return status;
    }

    public boolean retryable() {
        return switch (status.getCode()) {
            case RESOURCE_EXHAUSTED, FAILED_PRECONDITION, UNAVAILABLE, DEADLINE_EXCEEDED -> true;
            default -> false;
        };
    }
}
