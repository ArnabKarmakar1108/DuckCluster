package io.duckcluster.coordinator.http;

public enum ApiErrorCode {
    UNSUPPORTED_SQL(400),
    PLANNING_ERROR(400),
    BAD_REQUEST(400),
    NOT_FOUND(404),
    CANCELLED(499),
    RESOURCE_EXHAUSTED(429),
    TIMEOUT(504),
    NO_WORKERS(503),
    SERVICE_UNAVAILABLE(503),
    INTERNAL_ERROR(500);

    private final int httpStatus;

    ApiErrorCode(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public int httpStatus() {
        return httpStatus;
    }
}
