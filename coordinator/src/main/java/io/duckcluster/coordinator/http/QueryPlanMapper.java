package io.duckcluster.coordinator.http;

import io.duckcluster.common.model.BroadcastTable;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.planner.BroadcastSqlNames;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class QueryPlanMapper {
    private QueryPlanMapper() {}

    static Map<String, Object> toExplainResponse(String queryId, PlannedQuery plan) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("queryId", queryId);
        body.put("table", plan.tableName());
        body.put("mergeStrategy", plan.mergeStrategy().name());
        body.put("shardedTables", plan.shardedTables());
        body.put("broadcastTables", broadcastViews(queryId, plan.broadcastTables()));
        body.put("subqueryBroadcastTables", broadcastViews(queryId, plan.subqueryBroadcastTables()));
        body.put("correlatedCoPartitionTables", plan.correlatedCoPartitionTables());
        body.put("fragments", fragmentViews(queryId, plan.fragments()));
        return body;
    }

    private static List<Map<String, Object>> broadcastViews(String queryId, List<BroadcastTable> tables) {
        List<Map<String, Object>> views = new ArrayList<>();
        for (BroadcastTable table : tables) {
            Map<String, Object> view = new LinkedHashMap<>();
            view.put("table", table.tableName());
            view.put("shardCount", table.shardCount());
            view.put("workerTempTable", BroadcastSqlNames.tempTable(queryId, table.tableName()));
            views.add(view);
        }
        return views;
    }

    private static List<Map<String, Object>> fragmentViews(String queryId, List<FragmentSpec> fragments) {
        List<Map<String, Object>> views = new ArrayList<>();
        for (FragmentSpec fragment : fragments) {
            Map<String, Object> view = new LinkedHashMap<>();
            view.put("fragmentId", fragment.fragmentId());
            view.put("shardId", fragment.shardId());
            view.put("sql", fragment.sql());
            views.add(view);
        }
        return views;
    }
}
