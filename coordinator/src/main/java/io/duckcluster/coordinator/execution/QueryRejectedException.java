package io.duckcluster.coordinator.execution;

import io.duckcluster.coordinator.http.ApiErrorCode;
import io.duckcluster.coordinator.http.ApiException;

public final class QueryRejectedException extends ApiException {
    public QueryRejectedException(String message) {
        super(ApiErrorCode.RESOURCE_EXHAUSTED, message);
    }
}
