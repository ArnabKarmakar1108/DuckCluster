package io.duckcluster.coordinator.execution;

import io.duckcluster.coordinator.http.ApiErrorCode;
import io.duckcluster.coordinator.http.ApiException;

public final class QueryTimeoutException extends ApiException {
    public QueryTimeoutException(String message) {
        super(ApiErrorCode.TIMEOUT, message);
    }
}
