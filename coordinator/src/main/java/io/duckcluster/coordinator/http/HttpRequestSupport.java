package io.duckcluster.coordinator.http;

import io.javalin.http.Context;
import io.javalin.http.HttpStatus;

import java.util.Map;
import java.util.Optional;

final class HttpRequestSupport {
    private HttpRequestSupport() {}

    static Optional<String> sqlOrBadRequest(Context ctx, String sql) {
        if (sql == null || sql.isBlank()) {
            ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", "sql is required"));
            return Optional.empty();
        }
        return Optional.of(sql);
    }

    static Optional<Long> parseCatalogVersion(Context ctx, String raw) {
        try {
            long version = Long.parseLong(raw);
            if (version < 1) {
                ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", "version must be >= 1"));
                return Optional.empty();
            }
            return Optional.of(version);
        } catch (NumberFormatException e) {
            ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", "version must be a positive integer"));
            return Optional.empty();
        }
    }

    static Optional<Integer> parseShardId(Context ctx, String raw) {
        try {
            return Optional.of(Integer.parseInt(raw));
        } catch (NumberFormatException e) {
            ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", "shardId must be an integer"));
            return Optional.empty();
        }
    }
}
