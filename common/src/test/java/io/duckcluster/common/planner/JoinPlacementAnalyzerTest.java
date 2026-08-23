package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.ShuffleJoinTable;
import io.duckcluster.common.model.TableColumn;
import io.duckcluster.common.model.TableShardConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinPlacementAnalyzerTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"q03", "q05", "q07", "q08", "q09", "q10", "q12", "q18", "q21"})
    void tpchOrdersLineitemJoinsAreCoPartitioned(String queryId) throws Exception {
        PlannedQuery planned = planner.plan(loadTpch(queryId), TpchCatalog.create(), "q");

        List<String> sliced = List.of(planned.tableName(), planned.coPartitionJoinTables().isEmpty()
                ? "" : planned.coPartitionJoinTables().get(0));
        assertTrue(sliced.contains("orders") && sliced.contains("lineitem"),
                queryId + " driving=" + planned.tableName() + " coPartitioned=" + planned.coPartitionJoinTables());
        assertTrue(planned.broadcastTables().stream()
                        .noneMatch(table -> table.tableName().equals("orders") || table.tableName().equals("lineitem")),
                queryId + " broadcast=" + planned.broadcastTables());
        String sql = planned.fragments().get(2).sql();
        assertTrue(sql.contains("\"orders_shard2\".\"orders\""), sql);
        assertTrue(sql.contains("\"lineitem_shard2\".\"lineitem\""), sql);
    }

    @Test
    void joinOnNonKeyColumnFallsBackToBroadcast() {
        PlannedQuery planned = planner.plan(
                "SELECT COUNT(*) FROM lineitem l JOIN orders o ON l.l_suppkey = o.o_custkey",
                TpchCatalog.create());

        assertTrue(planned.coPartitionJoinTables().isEmpty());
        assertEquals(1, planned.broadcastTables().size());
    }

    @Test
    void equalityUnderOrDoesNotCoPartition() {
        PlannedQuery planned = planner.plan(
                "SELECT COUNT(*) FROM lineitem, orders "
                        + "WHERE l_orderkey = o_orderkey OR l_suppkey = o_custkey",
                TpchCatalog.create());

        assertTrue(planned.coPartitionJoinTables().isEmpty());
        assertEquals(1, planned.broadcastTables().size());
    }

    @Test
    void transitiveEqualityCoPartitions() {
        ClusterCatalog catalog = ClusterCatalog.withTables(Map.of(
                "a", new TableShardConfig("a", 4, "k"),
                "b", new TableShardConfig("b", 4, "k"),
                "c", new TableShardConfig("c", 4, "k")));
        catalog.replaceEstimatedRowCounts(Map.of("a", 1000L, "b", 10L, "c", 10L));

        PlannedQuery planned = planner.plan(
                "SELECT COUNT(*) FROM a, b, c WHERE a.k = b.k AND b.k = c.k", catalog);

        assertEquals("a", planned.tableName());
        assertTrue(planned.coPartitionJoinTables().containsAll(List.of("b", "c")));
        assertTrue(planned.broadcastTables().isEmpty());
    }

    @Test
    void selfJoinWithoutShardKeyEqualityIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> planner.plan(
                "SELECT COUNT(*) FROM lineitem l, orders o1, orders o2 "
                        + "WHERE l.l_orderkey = o1.o_orderkey AND o2.o_custkey = o1.o_custkey",
                TpchCatalog.create()));
    }

    @Test
    void undeclaredShardKeysNeverCoPartition() {
        ClusterCatalog catalog = new ClusterCatalog();
        catalog.registerTable("lineitem", 6);
        catalog.registerTable("orders", 6);

        PlannedQuery planned = planner.plan(
                "SELECT COUNT(*) FROM lineitem l JOIN orders o ON l.l_orderkey = o.o_orderkey", catalog);

        assertTrue(planned.coPartitionJoinTables().isEmpty());
        assertEquals(1, planned.broadcastTables().size());
    }

    @Test
    void workerRegistrationKeepsDeclaredShardKey() {
        ClusterCatalog catalog = new ClusterCatalog();
        catalog.declareTable(new TableShardConfig("orders", 2, "o_orderkey"));
        catalog.registerTable("orders", 6);

        assertEquals(6, catalog.table("orders").shardCount());
        assertEquals("o_orderkey", catalog.table("orders").shardKey());
    }

    @Test
    void differentlyPartitionedTableIsShuffledOnDrivingKey() {
        ClusterCatalog catalog = shuffleCatalog();

        PlannedQuery planned = planner.plan(
                "SELECT o_orderpriority, COUNT(*) FROM orders, lineitem "
                        + "WHERE o_orderkey = l_orderkey GROUP BY o_orderpriority",
                catalog, "q1");

        assertEquals("lineitem", planned.tableName());
        assertTrue(planned.broadcastTables().isEmpty(), planned.broadcastTables().toString());
        assertEquals(List.of(new ShuffleJoinTable("orders", 4, "o_orderkey", "BIGINT", 6)),
                planned.shuffleJoinTables());
        String sql = planned.fragments().get(3).sql();
        assertTrue(sql.contains("\"__dc_shuf_q1_orders_b3\""), sql);
        assertTrue(sql.contains("\"lineitem_shard3\".\"lineitem\""), sql);
    }

    @Test
    void shuffleRespectsMinimumRowThreshold() {
        CalciteQueryPlanner thresholdPlanner = new CalciteQueryPlanner(new PlannerOptions(true, true, 1_000_000));

        PlannedQuery planned = thresholdPlanner.plan(
                "SELECT COUNT(*) FROM orders, lineitem WHERE o_orderkey = l_orderkey", shuffleCatalog());

        assertTrue(planned.shuffleJoinTables().isEmpty());
        assertEquals(1, planned.broadcastTables().size());
    }

    @Test
    void shuffleRequiresJoinOnDrivingShardKey() {
        PlannedQuery planned = planner.plan(
                "SELECT COUNT(*) FROM orders, lineitem WHERE o_custkey = l_suppkey", shuffleCatalog());

        assertTrue(planned.shuffleJoinTables().isEmpty());
        assertEquals(1, planned.broadcastTables().size());
    }

    @Test
    void mismatchedKeyTypesShuffleInsteadOfCoPartition() {
        ClusterCatalog catalog = ClusterCatalog.withTables(Map.of(
                "lineitem", new TableShardConfig("lineitem", 6, "l_orderkey"),
                "orders", new TableShardConfig("orders", 6, "o_orderkey")));
        catalog.updateTableSchema("lineitem", List.of(new TableColumn("l_orderkey", "BIGINT")));
        catalog.updateTableSchema("orders", List.of(new TableColumn("o_orderkey", "INTEGER")));
        catalog.replaceEstimatedRowCounts(Map.of("lineitem", 600L, "orders", 150L));

        PlannedQuery planned = planner.plan(
                "SELECT COUNT(*) FROM lineitem l JOIN orders o ON l.l_orderkey = o.o_orderkey", catalog);

        assertTrue(planned.coPartitionJoinTables().isEmpty());
        assertEquals("BIGINT", planned.shuffleJoinTables().get(0).joinKeyType());
    }

    @Test
    void correlatedExistsIsShardLocalOnlyWhenCoPartitioned() throws Exception {
        PlannedQuery coLocated = planner.plan(loadTpch("q04"), TpchCatalog.create());
        assertTrue(coLocated.correlatedCoPartitionTables().contains("lineitem"));

        PlannedQuery global = planner.plan(loadTpch("q04"), shuffleCatalog());
        assertTrue(global.correlatedCoPartitionTables().isEmpty());
        String sql = global.fragments().get(0).sql();
        assertFalse(sql.contains("\"lineitem_shard0\".\"lineitem\" WHERE"), sql);
        assertTrue(sql.contains("__dc_bcast_") || sql.contains("UNION ALL"), sql);
    }

    @Test
    void versionedBroadcastTableNameMatchesWorkerMaterialization() {
        ClusterCatalog catalog = ClusterCatalog.withTables(Map.of(
                "lineitem", new TableShardConfig("lineitem", 6, "l_orderkey"),
                "customer", new TableShardConfig("customer", 2, "c_custkey")));
        catalog.setActiveDataVersion("customer", 3);

        PlannedQuery planned = planner.plan(
                "SELECT c.c_name, COUNT(*) FROM lineitem l JOIN customer c ON l.l_custkey = c.c_custkey "
                        + "GROUP BY c.c_name",
                catalog, "q9");

        String sql = planned.fragments().get(0).sql();
        assertTrue(sql.contains(BroadcastSqlNames.versionedTable(3, "customer")), sql);
        assertFalse(sql.contains("__dc_bcast_q9_customer"), sql);
    }

    /** lineitem keeps TPC-H layout; orders is split 4 ways by customer, so it is not co-partitioned. */
    private static ClusterCatalog shuffleCatalog() {
        ClusterCatalog catalog = ClusterCatalog.withTables(Map.of(
                "lineitem", new TableShardConfig("lineitem", 6, "l_orderkey"),
                "orders", new TableShardConfig("orders", 4, "o_custkey")));
        catalog.updateTableSchema("lineitem", List.of(
                new TableColumn("l_orderkey", "BIGINT"),
                new TableColumn("l_suppkey", "BIGINT"),
                new TableColumn("l_commitdate", "DATE"),
                new TableColumn("l_receiptdate", "DATE")));
        catalog.updateTableSchema("orders", List.of(
                new TableColumn("o_orderkey", "BIGINT"),
                new TableColumn("o_custkey", "BIGINT"),
                new TableColumn("o_orderdate", "DATE"),
                new TableColumn("o_orderpriority", "VARCHAR")));
        catalog.replaceEstimatedRowCounts(Map.of("lineitem", 60_000L, "orders", 15_000L));
        return catalog;
    }

    private static String loadTpch(String queryId) throws Exception {
        Path path = Path.of("..", "tests", "integration", "queries", "tpch", queryId + ".sql");
        if (!Files.exists(path)) {
            path = Path.of("tests", "integration", "queries", "tpch", queryId + ".sql");
        }
        String sql = Files.readString(path).strip();
        return sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql;
    }
}
