package io.duckcluster.coordinator.execution;

import io.duckcluster.common.model.QueryResult;

import java.util.List;

/** Helpers for async query result spools (4.6). */
final class AsyncQuerySpoolSupport {
    private AsyncQuerySpoolSupport() {}

    static QueryResult toClientResult(QueryResultSpool spool, QueryResult merged) {
        if (spool == null) {
            return merged;
        }
        if (!merged.rows().isEmpty()) {
            spool.writeAll(merged.columns(), merged.rows());
        }
        if (spool.rowCount() > 0) {
            return new QueryResult(merged.queryId(), spool.columns(), List.of(), merged.stats());
        }
        return merged;
    }
}
