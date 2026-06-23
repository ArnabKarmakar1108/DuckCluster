package io.duckcluster.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DuckClusterClient implements AutoCloseable {

    private final HttpClient httpClient;
    private final String queryUrl;
    private final ObjectMapper mapper = new ObjectMapper();

    public DuckClusterClient(String coordinatorBaseUrl) {
        String base = coordinatorBaseUrl.endsWith("/")
                ? coordinatorBaseUrl.substring(0, coordinatorBaseUrl.length() - 1)
                : coordinatorBaseUrl;
        this.queryUrl = base + "/v1/query";
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    public QueryExecution execute(String sql) throws Exception {
        long start = System.nanoTime();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(queryUrl))
                .timeout(Duration.ofHours(2))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(Map.of("sql", sql))))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        if (response.statusCode() >= 400) {
            throw new IllegalStateException("DuckCluster query failed (" + response.statusCode() + "): " + response.body());
        }

        JsonNode root = mapper.readTree(response.body());
        List<String> columns = mapper.convertValue(root.get("columns"), mapper.getTypeFactory().constructCollectionType(List.class, String.class));
        List<List<Object>> rows = mapper.convertValue(root.get("rows"), mapper.getTypeFactory().constructCollectionType(List.class, List.class));

        Map<String, Object> detail = new LinkedHashMap<>();
        long executionMs = elapsedMs;
        JsonNode stats = root.get("stats");
        if (stats != null && !stats.isNull()) {
            detail.put("mergeStrategy", textOrNull(stats, "mergeStrategy"));
            executionMs = longOrZero(stats, "durationMs");
            detail.put("durationMs", executionMs);
            detail.put("fragmentsExecuted", intOrZero(stats, "fragmentsExecuted"));
            detail.put("workersUsed", intOrZero(stats, "workersUsed"));
            if (stats.has("workerDurationsMs") && stats.get("workerDurationsMs").isObject()) {
                detail.put("workerDurationsMs", mapper.convertValue(stats.get("workerDurationsMs"), Map.class));
            }
        }

        return new QueryExecution(elapsedMs, executionMs, columns, rows, detail);
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static long longOrZero(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? 0L : value.asLong();
    }

    private static int intOrZero(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? 0 : value.asInt();
    }

    @Override
    public void close() {
        // HttpClient has no close hook in Java 17.
    }

    public record QueryExecution(
            long latencyMs,
            long executionMs,
            List<String> columns,
            List<List<Object>> rows,
            Map<String, Object> detail) {}
}
