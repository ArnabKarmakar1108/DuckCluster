package io.duckcluster.coordinator.http;

import io.duckcluster.common.model.QueryResult;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class QueryResponseMapper {
    private QueryResponseMapper() {}

    static Map<String, Object> toFullResponse(QueryResult result) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("queryId", result.queryId());
        response.put("columns", result.columns());
        response.put("rows", result.rows());
        response.put("stats", toStatsMap(result.stats()));
        return response;
    }

    static Map<String, Object> toStatsMap(QueryResult.QueryStats stats) {
        Map<String, Object> statsMap = new LinkedHashMap<>();
        statsMap.put("mergeStrategy", stats.mergeStrategy().name());
        statsMap.put("workersUsed", stats.workersUsed());
        statsMap.put("fragmentsExecuted", stats.fragmentsExecuted());
        statsMap.put("durationMs", stats.durationMs());
        statsMap.put("workerDurationsMs", stats.workerDurationsMs());
        return statsMap;
    }

    static Map<String, Object> toResultChunk(
            QueryResult result,
            int offset,
            int limit,
            List<List<Object>> rows,
            Integer nextOffset,
            long totalRowCount) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("queryId", result.queryId());
        body.put("columns", result.columns());
        body.put("rows", rows);
        body.put("offset", offset);
        body.put("limit", limit);
        long rowCount = totalRowCount >= 0 ? totalRowCount : result.rows().size();
        body.put("rowCount", rowCount);
        body.put("nextOffset", nextOffset);
        if (result.stats() != null) {
            body.put("stats", toStatsMap(result.stats()));
        }
        return body;
    }
}
