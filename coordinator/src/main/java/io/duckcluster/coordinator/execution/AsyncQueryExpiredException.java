package io.duckcluster.coordinator.execution;

public final class AsyncQueryExpiredException extends RuntimeException {
    private final String queryId;

    public AsyncQueryExpiredException(String queryId) {
        super("query result expired: " + queryId);
        this.queryId = queryId;
    }

    public String queryId() {
        return queryId;
    }
}
