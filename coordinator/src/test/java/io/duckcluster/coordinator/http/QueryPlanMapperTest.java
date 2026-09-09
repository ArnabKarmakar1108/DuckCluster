package io.duckcluster.coordinator.http;

import io.duckcluster.common.model.BroadcastTable;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TableShardConfig;
import io.duckcluster.common.planner.BroadcastSqlNames;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryPlanMapperTest {

    @Test
    void scopesBroadcastTempTablesPerQuery() {
        PlannedQuery plan = new PlannedQuery(
                "SELECT * FROM lineitem JOIN orders ON ...",
                List.of("lineitem"),
                List.of(new BroadcastTable("orders", 2)),
                List.of(new FragmentSpec(0, 0, "SELECT 1", MergeStrategyType.CONCATENATE)),
                MergeStrategyType.CONCATENATE,
                QueryAnalysis.empty(),
                List.of(),
                null);

        Map<String, Object> body = QueryPlanMapper.toExplainResponse("query-a", plan);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> broadcasts = (List<Map<String, Object>>) body.get("broadcastTables");
        assertEquals("__dc_bcast_query-a_orders", broadcasts.get(0).get("workerTempTable"));

        Map<String, Object> other = QueryPlanMapper.toExplainResponse("query-b", plan);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> otherBroadcasts = (List<Map<String, Object>>) other.get("broadcastTables");
        assertEquals("__dc_bcast_query-b_orders", otherBroadcasts.get(0).get("workerTempTable"));
    }

    @Test
    void explainUsesVersionedBroadcastNameWhenTableHasActiveVersion() {
        ClusterCatalog catalog = ClusterCatalog.withTables(Map.of(
                "orders", new TableShardConfig("orders", 2, "o_orderkey")));
        catalog.setActiveDataVersion("orders", 4);

        PlannedQuery plan = new PlannedQuery(
                "SELECT * FROM lineitem JOIN orders ON ...",
                List.of("lineitem"),
                List.of(new BroadcastTable("orders", 2)),
                List.of(new FragmentSpec(0, 0, "SELECT 1", MergeStrategyType.CONCATENATE)),
                MergeStrategyType.CONCATENATE,
                QueryAnalysis.empty(),
                List.of(),
                null);

        Map<String, Object> body = QueryPlanMapper.toExplainResponse("query-z", plan, catalog);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> broadcasts = (List<Map<String, Object>>) body.get("broadcastTables");
        assertEquals(BroadcastSqlNames.versionedTable(4, "orders"), broadcasts.get(0).get("workerTempTable"));
        assertEquals(4L, broadcasts.get(0).get("activeDataVersion"));
        assertTrue(broadcasts.get(0).get("workerTempTable").toString().contains("_v4_"));
    }
}
