package io.duckcluster.coordinator.execution;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FragmentExecutionExceptionTest {

    @Test
    void marksPoolAndCacheErrorsRetryable() {
        assertTrue(exception(Status.RESOURCE_EXHAUSTED).retryable());
        assertTrue(exception(Status.FAILED_PRECONDITION).retryable());
        assertTrue(exception(Status.UNAVAILABLE).retryable());
        assertTrue(exception(Status.DEADLINE_EXCEEDED).retryable());
    }

    @Test
    void marksInternalErrorsNonRetryable() {
        assertFalse(exception(Status.INTERNAL).retryable());
        assertFalse(exception(Status.INVALID_ARGUMENT).retryable());
    }

    private static FragmentExecutionException exception(Status status) {
        return new FragmentExecutionException(status, status.asRuntimeException());
    }
}
