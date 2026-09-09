package io.duckcluster.coordinator.execution;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Callbacks for streaming merge output into an async result spool (4.6). */
public final class MergeSpoolHooks {
    public interface RowConsumer {
        void setColumns(List<String> columns);

        void addRow(List<Object> row);

        void finish();
    }

    private static final ConcurrentHashMap<String, RowConsumer> ACTIVE = new ConcurrentHashMap<>();

    private MergeSpoolHooks() {}

    public static void attach(String queryId, RowConsumer consumer) {
        if (queryId != null && consumer != null) {
            ACTIVE.put(queryId, consumer);
        }
    }

    public static Optional<RowConsumer> get(String queryId) {
        return Optional.ofNullable(ACTIVE.get(queryId));
    }

    public static void remove(String queryId) {
        if (queryId != null) {
            ACTIVE.remove(queryId);
        }
    }

    static RowConsumer forSpool(QueryResultSpool spool) {
        return new RowConsumer() {
            @Override
            public void setColumns(List<String> columns) {
                spool.writeAll(columns, List.of());
            }

            @Override
            public void addRow(List<Object> row) {
                spool.appendRow(row);
            }

            @Override
            public void finish() {
                try {
                    spool.flush();
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to flush merge spool", e);
                }
            }
        };
    }
}
