package io.duckcluster.coordinator.execution;

import io.duckcluster.common.model.QueryResult;

import java.util.List;

final class QueryResultSizeEstimator {
    private static final long BASE_OVERHEAD_BYTES = 256;

    private QueryResultSizeEstimator() {}

    static long estimateBytes(QueryResult result) {
        return estimateBytes(result, 0L);
    }

    static long estimateBytes(QueryResult result, long spoolBytes) {
        if (result == null) {
            return 0;
        }
        if (spoolBytes > 0) {
            return spoolBytes + BASE_OVERHEAD_BYTES;
        }
        long bytes = BASE_OVERHEAD_BYTES;
        for (String column : result.columns()) {
            bytes += column == null ? 0 : column.length() * 2L;
        }
        for (List<Object> row : result.rows()) {
            for (Object cell : row) {
                bytes += cell == null ? 4 : String.valueOf(cell).length() * 2L;
            }
        }
        return bytes;
    }
}
