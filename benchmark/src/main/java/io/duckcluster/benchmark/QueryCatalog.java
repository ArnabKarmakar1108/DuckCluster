package io.duckcluster.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public final class QueryCatalog {

    private final List<QuerySpec> queries;

    public QueryCatalog(Path queriesDir) throws IOException {
        if (!Files.isDirectory(queriesDir)) {
            throw new IllegalArgumentException("Queries directory not found: " + queriesDir);
        }

        List<QuerySpec> loaded = new ArrayList<>();
        try (Stream<Path> paths = Files.list(queriesDir)) {
            paths.filter(path -> path.getFileName().toString().matches("Q\\d{2}\\.sql"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .forEach(path -> {
                        String queryId = path.getFileName().toString().replace(".sql", "");
                        try {
                            String sql = Files.readString(path).trim();
                            sql = stripTrailingSemicolon(sql);
                            loaded.add(new QuerySpec(queryId, sql, true));
                        } catch (IOException exception) {
                            throw new IllegalStateException("Failed to read query file: " + path, exception);
                        }
                    });
        }

        if (loaded.isEmpty()) {
            throw new IllegalArgumentException("No Qxx.sql query files found in " + queriesDir);
        }
        this.queries = List.copyOf(loaded);
    }

    public List<QuerySpec> all() {
        return queries;
    }

    public List<QuerySpec> supported() {
        return queries.stream().filter(QuerySpec::supported).toList();
    }

    public Map<String, Object> unsupportedSummary() {
        Map<String, Object> summary = new LinkedHashMap<>();
        for (QuerySpec query : queries) {
            if (!query.supported()) {
                summary.put(query.id(), Map.of(
                        "status", "UNSUPPORTED",
                        "reason", "correlated subquery not supported by planner"));
            }
        }
        return summary;
    }

    private static String stripTrailingSemicolon(String sql) {
        String trimmed = sql.strip();
        if (trimmed.endsWith(";")) {
            return trimmed.substring(0, trimmed.length() - 1).strip();
        }
        return trimmed;
    }

    public record QuerySpec(String id, String sql, boolean supported) {}
}
