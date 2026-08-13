package io.duckcluster.worker.duckdb;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Tracks in-flight fragment statements so coordinator cancel can stop DuckDB work. */
public final class RunningFragmentRegistry {
    private final Map<String, Statement> statementsByKey = new ConcurrentHashMap<>();

    public void register(String runKey, Statement statement) {
        statementsByKey.put(runKey, statement);
    }

    public void unregister(String runKey) {
        statementsByKey.remove(runKey);
    }

    public int cancelQuery(String queryId) {
        int cancelled = 0;
        for (Map.Entry<String, Statement> entry : statementsByKey.entrySet()) {
            if (!entry.getKey().startsWith(queryId + ":")) {
                continue;
            }
            try {
                entry.getValue().cancel();
                cancelled++;
            } catch (SQLException ignored) {
                // Statement may already be closed.
            }
        }
        return cancelled;
    }
}
