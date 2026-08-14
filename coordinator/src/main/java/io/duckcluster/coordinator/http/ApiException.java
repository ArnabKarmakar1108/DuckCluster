package io.duckcluster.coordinator.http;

public class ApiException extends RuntimeException {
    private final ApiErrorCode code;

    public ApiException(ApiErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ApiException(ApiErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ApiErrorCode code() {
        return code;
    }
}
