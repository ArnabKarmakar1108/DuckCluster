package io.duckcluster.coordinator.execution;

import io.duckcluster.coordinator.http.ApiErrorCode;
import io.duckcluster.coordinator.http.ApiException;

public final class QueryCancelledException extends ApiException {
    private final String queryId;

    public QueryCancelledException(String queryId) {
        super(ApiErrorCode.CANCELLED, "query cancelled: " + queryId);
        this.queryId = queryId;
    }

    public String queryId() {
        return queryId;
    }
}
