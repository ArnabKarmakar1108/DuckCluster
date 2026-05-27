package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.TableShardConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpchImplicitJoinTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final io.duckcluster.common.model.ClusterCatalog tpchCatalog = TpchCatalog.create();

    @Test
    void plansQ05CommaJoinWithShardRewrite() throws Exception {
        String sql = loadQuery("Q05.sql");
        PlannedQuery planned = planner.plan(sql, tpchCatalog);

        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertEquals("lineitem", planned.tableName());
        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("lineitem_shard0"), fragmentSql);
        assertTrue(fragmentSql.contains("customer") || fragmentSql.contains("UNION ALL"), fragmentSql);
        assertTrue(fragmentSql.contains("n_name"), fragmentSql);
    }

    @Test
    void plansQ12OrdersLineitemCommaJoin() throws Exception {
        String sql = loadQuery("Q12.sql");
        PlannedQuery planned = planner.plan(sql, tpchCatalog);

        assertEquals("lineitem", planned.tableName());
        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("lineitem_shard0"), fragmentSql);
        assertTrue(fragmentSql.contains("orders") || fragmentSql.contains("UNION ALL"), fragmentSql);
    }

    @Test
    void collectsTablesFromAliasedCommaJoin() throws Exception {
        String sql = """
                SELECT n1.n_name, SUM(l.l_extendedprice) AS revenue
                FROM lineitem l, nation n1, region r
                WHERE l.l_nationkey = n1.n_nationkey
                  AND n1.n_regionkey = r.r_regionkey
                GROUP BY n1.n_name
                """;
        var parsed = planner.parse(sql);
        var select = (org.apache.calcite.sql.SqlSelect) parsed;
        var tables = CalciteQueryPlanner.collectTableNames(select.getFrom());

        assertTrue(tables.contains("lineitem"));
        assertTrue(tables.contains("nation"));
        assertTrue(tables.contains("region"));
        assertEquals(3, tables.size());
    }

    @Test
    void preservesBroadcastAliasOnUnionAll() {
        ClusterCatalog catalog = io.duckcluster.common.model.ClusterCatalog.withTables(Map.of(
                "lineitem", new io.duckcluster.common.model.TableShardConfig("lineitem", 6, "l_orderkey"),
                "customer", new io.duckcluster.common.model.TableShardConfig("customer", 3, "c_custkey")));

        PlannedQuery planned = planner.plan(
                """
                SELECT c.c_name, SUM(l.l_extendedprice) AS total
                FROM lineitem AS l, customer AS c
                WHERE l.l_custkey = c.c_custkey
                GROUP BY c.c_name
                """,
                catalog);

        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("UNION ALL"), fragmentSql);
        assertTrue(fragmentSql.contains(" AS \"c\"") || fragmentSql.contains(" AS \"l\""), fragmentSql);
    }

    private static String loadQuery(String name) throws Exception {
        Path path = Path.of("..", "benchmark", "src", "main", "resources", "queries", name).normalize();
        if (!Files.exists(path)) {
            path = Path.of("benchmark", "src", "main", "resources", "queries", name);
        }
        String sql = Files.readString(path);
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return sql;
    }
}
