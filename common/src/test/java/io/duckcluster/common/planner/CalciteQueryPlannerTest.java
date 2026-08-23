package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.TableShardConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CalciteQueryPlannerTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final ClusterCatalog catalog = ClusterCatalog.demo(3);

    @Test
    void detectsTopKStrategy() {
        MergeStrategyType strategy = planner.detectMergeStrategy(
                planner.parse("SELECT id, score FROM events ORDER BY score DESC LIMIT 10"));
        assertEquals(MergeStrategyType.TOP_K, strategy);
    }

    @Test
    void planPushesOrderByAndLimitToFragments() {
        PlannedQuery planned = planner.plan(
                "SELECT id, score FROM events ORDER BY score DESC LIMIT 2", catalog);
        assertEquals(MergeStrategyType.TOP_K, planned.mergeStrategy());
        assertEquals(2, planned.topK().limit());
        assertEquals(1, planned.topK().orderBy().size());
        assertEquals("score", planned.topK().orderBy().get(0).column());
        assertTrue(planned.topK().orderBy().get(0).descending());
        assertTrue(planned.fragments().get(0).sql().contains("ORDER BY"));
        assertTrue(planned.fragments().get(0).sql().contains("LIMIT 2"));
    }

    @Test
    void groupByFragmentsDecomposeAvgIntoSumAndCount() {
        PlannedQuery planned = planner.plan(
                "SELECT category, AVG(score) AS avg_score FROM events GROUP BY category", catalog);
        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertEquals(2, planned.analysis().aggregates().size());
        String sql = planned.fragments().get(0).sql();
        assertTrue(sql.contains("SUM"), sql);
        assertTrue(sql.contains("COUNT"), sql);
        assertTrue(sql.contains("__dc_agg_0_sum"), sql);
        assertTrue(sql.contains("__dc_agg_0_cnt"), sql);
    }

    @Test
    void detectsConcatenateStrategyForSimpleSelect() {
        MergeStrategyType strategy = planner.detectMergeStrategy(planner.parse("SELECT * FROM events"));
        assertEquals(MergeStrategyType.CONCATENATE, strategy);
    }

    @Test
    void detectsGroupByMergeStrategy() {
        MergeStrategyType strategy = planner.detectMergeStrategy(
                planner.parse("SELECT category, COUNT(*) FROM events GROUP BY category"));
        assertEquals(MergeStrategyType.GROUP_BY_MERGE, strategy);
    }

    @Test
    void planGeneratesOneFragmentPerShard() {
        PlannedQuery planned = planner.plan("SELECT * FROM events", catalog);
        assertEquals(MergeStrategyType.CONCATENATE, planned.mergeStrategy());
        assertEquals(3, planned.fragments().size());
        assertTrue(planned.fragments().get(0).sql().contains("events_shard0"));
        assertTrue(planned.fragments().get(1).sql().contains("events_shard1"));
        assertTrue(planned.fragments().get(2).sql().contains("events_shard2"));
    }

    @Test
    void planUsesQualifiedTableNameWithFilter() {
        PlannedQuery planned = planner.plan("SELECT * FROM events WHERE id > 2", catalog);
        String sql = planned.fragments().get(0).sql();
        assertTrue(sql.contains("> 2"));
        assertTrue(sql.contains("events_shard0"));
    }

    @Test
    void groupByFragmentsKeepAggregatePushdown() {
        PlannedQuery planned = planner.plan("SELECT category, COUNT(*) AS cnt FROM events GROUP BY category", catalog);
        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertEquals(1, planned.analysis().aggregates().size());
        assertTrue(planned.fragments().get(0).sql().contains("GROUP BY"));
        assertTrue(planned.fragments().get(0).sql().contains("__dc_agg_0"));
    }

    @Test
    void partialAggFragmentsIncludeAggregateAliases() {
        PlannedQuery planned = planner.plan("SELECT COUNT(*), SUM(id) FROM events", catalog);
        assertEquals(MergeStrategyType.PARTIAL_AGG, planned.mergeStrategy());
        assertTrue(planned.fragments().get(1).sql().contains("__dc_agg_0"));
        assertTrue(planned.fragments().get(1).sql().contains("__dc_agg_1"));
    }

    @Test
    void planBroadcastsTableWithFewerShards() {
        ClusterCatalog joinCatalog = ClusterCatalog.withTables(Map.of(
                "lineitem", new TableShardConfig("lineitem", 6, "l_orderkey"),
                "customer", new TableShardConfig("customer", 2, "c_custkey")));

        PlannedQuery planned = planner.plan(
                "SELECT c.c_name, SUM(l.l_extendedprice) FROM lineitem l JOIN customer c ON l.l_custkey = c.c_custkey GROUP BY c.c_name",
                joinCatalog);

        assertEquals(6, planned.fragments().size());
        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertEquals("lineitem", planned.tableName());
        assertEquals(1, planned.broadcastTables().size());
        assertEquals("customer", planned.broadcastTables().get(0).tableName());
        assertEquals(2, planned.broadcastTables().get(0).shardCount());

        String sql0 = planned.fragments().get(0).sql();
        assertTrue(sql0.contains("\"lineitem_shard0\".\"lineitem\""), "Driving table shard-qualified: " + sql0);
        assertTrue(sql0.contains("__dc_bcast_customer"), "Broadcast table materialized: " + sql0);
        assertTrue(!sql0.contains("UNION ALL"), "Broadcast avoids per-fragment UNION ALL: " + sql0);
        assertTrue(sql0.contains("GROUP BY"), "Preserves GROUP BY: " + sql0);
    }

    @Test
    void planBroadcastsWithSameShardCount() {
        ClusterCatalog joinCatalog = ClusterCatalog.withTables(Map.of(
                "lineitem", new TableShardConfig("lineitem", 4, "l_orderkey"),
                "orders", new TableShardConfig("orders", 4, "o_orderkey")));

        PlannedQuery planned = planner.plan(
                "SELECT o.o_orderpriority, COUNT(*) FROM lineitem l JOIN orders o ON l.l_orderkey = o.o_orderkey GROUP BY o.o_orderpriority",
                joinCatalog);

        assertEquals(4, planned.fragments().size());
        // Tied shard counts: last table in FROM traversal order drives.
        assertEquals("orders", planned.tableName());
        assertTrue(planned.broadcastTables().isEmpty());
        assertTrue(planned.coPartitionJoinTables().contains("lineitem"));

        String sql0 = planned.fragments().get(0).sql();
        assertTrue(sql0.contains("\"orders_shard0\".\"orders\""), "Driving table: " + sql0);
        assertTrue(sql0.contains("\"lineitem_shard0\""), "Co-partitioned lineitem shard: " + sql0);
        assertTrue(!sql0.contains("__dc_bcast_"), "No broadcast for co-partition join: " + sql0);
    }

    @Test
    void planScopesBroadcastTempTablesToQueryId() {
        ClusterCatalog joinCatalog = ClusterCatalog.withTables(Map.of(
                "lineitem", new TableShardConfig("lineitem", 6, "l_orderkey"),
                "customer", new TableShardConfig("customer", 2, "c_custkey")));

        PlannedQuery planned = planner.plan(
                "SELECT c.c_name, SUM(l.l_extendedprice) FROM lineitem l "
                        + "JOIN customer c ON l.l_custkey = c.c_custkey GROUP BY c.c_name",
                joinCatalog,
                "query-42");

        String sql0 = planned.fragments().get(0).sql();
        assertTrue(sql0.contains("__dc_bcast_query-42_customer"),
                "Broadcast table is query-scoped: " + sql0);
        assertTrue(!sql0.contains("UNION ALL"), "Broadcast avoids per-fragment UNION ALL: " + sql0);
    }

    @Test
    void planHandlesDifferentShardCounts() {
        ClusterCatalog joinCatalog = ClusterCatalog.withTables(Map.of(
                "lineitem", new TableShardConfig("lineitem", 6, "l_orderkey"),
                "orders", new TableShardConfig("orders", 4, "o_orderkey")));

        PlannedQuery planned = planner.plan(
                "SELECT * FROM lineitem l JOIN orders o ON l.l_orderkey = o.o_orderkey", joinCatalog);

        assertEquals(6, planned.fragments().size());
        assertEquals("lineitem", planned.tableName());
        assertEquals(1, planned.broadcastTables().size());
        assertEquals("orders", planned.broadcastTables().get(0).tableName());
        assertEquals(4, planned.broadcastTables().get(0).shardCount());
    }

    @Test
    void planRejectsQueryWithUnknownTable() {
        assertThrows(IllegalArgumentException.class, () ->
                planner.plan("SELECT * FROM unknown_table", catalog));
    }

    @Test
    void planPreservesAliasesInBroadcastJoin() {
        ClusterCatalog joinCatalog = ClusterCatalog.withTables(Map.of(
                "lineitem", new TableShardConfig("lineitem", 2, "l_orderkey"),
                "customer", new TableShardConfig("customer", 1, "c_custkey")));

        PlannedQuery planned = planner.plan(
                "SELECT l.l_quantity FROM lineitem l JOIN customer c ON l.l_custkey = c.c_custkey WHERE l.l_quantity > 10",
                joinCatalog);

        String sql0 = planned.fragments().get(0).sql();
        assertTrue(sql0.contains("\"lineitem_shard0\".\"lineitem\""), "Driving table qualified: " + sql0);
        assertTrue(sql0.contains("\"customer_shard0\".\"customer\""), "Broadcast customer: " + sql0);
        assertTrue(sql0.contains("> 10"), "Preserves WHERE: " + sql0);
    }

    @Test
    void planHandlesThreeWayJoin() {
        ClusterCatalog joinCatalog = ClusterCatalog.withTables(Map.of(
                "lineitem", new TableShardConfig("lineitem", 3, "l_orderkey"),
                "supplier", new TableShardConfig("supplier", 1, "s_suppkey"),
                "nation", new TableShardConfig("nation", 1, "n_nationkey")));

        PlannedQuery planned = planner.plan(
                "SELECT n.n_name, SUM(l.l_extendedprice) FROM lineitem l JOIN supplier s ON l.l_suppkey = s.s_suppkey JOIN nation n ON s.s_nationkey = n.n_nationkey GROUP BY n.n_name",
                joinCatalog);

        assertEquals(3, planned.fragments().size());
        assertEquals("lineitem", planned.tableName());
        assertEquals(2, planned.broadcastTables().size());

        String sql0 = planned.fragments().get(0).sql();
        assertTrue(sql0.contains("\"lineitem_shard0\".\"lineitem\""), "Driving table: " + sql0);
        assertTrue(sql0.contains("\"supplier_shard0\".\"supplier\""), "Broadcast supplier: " + sql0);
        assertTrue(sql0.contains("\"nation_shard0\".\"nation\""), "Broadcast nation: " + sql0);
    }

    @Test
    void drivingTablePrefersLargerRowEstimateAmongSameShardCount() {
        ClusterCatalog joinCatalog = ClusterCatalog.withTables(Map.of(
                "orders", new TableShardConfig("orders", 3, "o_orderkey"),
                "lineitem", new TableShardConfig("lineitem", 3, "l_orderkey")));
        joinCatalog.replaceEstimatedRowCounts(Map.of("orders", 1_000L, "lineitem", 50_000L));

        PlannedQuery planned = planner.plan(
                "SELECT o.o_orderkey, l.l_linenumber FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey",
                joinCatalog);

        assertEquals("lineitem", planned.tableName());
        assertTrue(planned.broadcastTables().isEmpty());
        assertTrue(planned.coPartitionJoinTables().contains("orders"));
    }

    @Test
    void selfJoinOnShardKeyIsAllowed() {
        PlannedQuery planned = planner.plan(
                "SELECT COUNT(*) FROM events e1 JOIN events e2 ON e1.id = e2.id",
                catalog);
        assertEquals("events", planned.tableName());
        assertTrue(planned.fragments().get(0).sql().contains("\"events_shard0\".\"events\""));
    }

    @Test
    void selfJoinOnNonKeyColumnIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> planner.plan(
                "SELECT COUNT(*) FROM events e1 JOIN events e2 ON e1.category = e2.category",
                catalog));
    }

    @Test
    void planDetectsMergeStrategyForJoinQuery() {
        ClusterCatalog joinCatalog = ClusterCatalog.withTables(Map.of(
                "orders", new TableShardConfig("orders", 3, "o_orderkey"),
                "customer", new TableShardConfig("customer", 1, "c_custkey")));

        PlannedQuery planned = planner.plan(
                "SELECT c.c_name, COUNT(*) FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY c.c_name",
                joinCatalog);

        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
    }
}
