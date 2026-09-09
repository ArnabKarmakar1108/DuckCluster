package io.duckcluster.coordinator.http;

import io.duckcluster.common.model.BroadcastTable;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.ShuffleJoinTable;
import io.duckcluster.common.planner.BroadcastSqlNames;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class QueryPlanMapper {
    private QueryPlanMapper() {}

    static Map<String, Object> toExplainResponse(String queryId, PlannedQuery plan) {
        return toExplainResponse(queryId, plan, null);
    }

    static Map<String, Object> toExplainResponse(String queryId, PlannedQuery plan, ClusterCatalog catalog) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("queryId", queryId);
        body.put("table", plan.tableName());
        body.put("mergeStrategy", plan.mergeStrategy().name());
        body.put("shardedTables", plan.shardedTables());
        body.put("broadcastTables", broadcastViews(queryId, plan.broadcastTables(), catalog));
        body.put("subqueryBroadcastTables", broadcastViews(queryId, plan.subqueryBroadcastTables(), catalog));
        body.put("correlatedCoPartitionTables", plan.correlatedCoPartitionTables());
        body.put("coPartitionJoinTables", plan.coPartitionJoinTables());
        body.put("shuffleJoinTables", shuffleViews(queryId, plan.shuffleJoinTables()));
        body.put("fragments", fragmentViews(queryId, plan.fragments()));
        return body;
    }

    private static List<Map<String, Object>> broadcastViews(
            String queryId, List<BroadcastTable> tables, ClusterCatalog catalog) {
        List<Map<String, Object>> views = new ArrayList<>();
        for (BroadcastTable table : tables) {
            Map<String, Object> view = new LinkedHashMap<>();
            view.put("table", table.tableName());
            view.put("shardCount", table.shardCount());
            long version = catalog == null ? 0L : catalog.activeDataVersion(table.tableName());
            view.put("workerTempTable", BroadcastSqlNames.broadcastTable(queryId, table.tableName(), version));
            if (version > 0) {
                view.put("activeDataVersion", version);
            }
            views.add(view);
        }
        return views;
    }

    private static List<Map<String, Object>> shuffleViews(String queryId, List<ShuffleJoinTable> tables) {
        List<Map<String, Object>> views = new ArrayList<>();
        for (ShuffleJoinTable table : tables) {
            Map<String, Object> view = new LinkedHashMap<>();
            view.put("table", table.tableName());
            view.put("sourceShardCount", table.sourceShardCount());
            view.put("joinColumn", table.joinColumn());
            view.put("joinKeyType", table.joinKeyType());
            view.put("bucketCount", table.bucketCount());
            view.put("workerBucketTable", BroadcastSqlNames.shuffleBucketTable(queryId, table.tableName(), 0)
                    .replaceFirst("_b0$", "_b{shard}"));
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
