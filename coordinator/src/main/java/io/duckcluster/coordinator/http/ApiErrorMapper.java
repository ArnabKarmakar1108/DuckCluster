package io.duckcluster.coordinator.http;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ApiErrorMapper {
    private ApiErrorMapper() {}

    public static Map<String, Object> body(ApiException exception) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", exception.code().name());
        body.put("error", exception.getMessage());
        return body;
    }
}
