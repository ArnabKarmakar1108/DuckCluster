package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpchDerivedTableTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final ClusterCatalog tpchCatalog = TpchCatalog.create();

    @Test
    void collectsTablesInsideDerivedTable() {
        String sql = """
                SELECT supp_nation, sum(volume) AS revenue
                FROM (
                    SELECT n1.n_name AS supp_nation, l_extendedprice AS volume
                    FROM supplier, lineitem, nation n1
                ) AS shipping
                GROUP BY supp_nation
                """;
        SqlNode parsed = planner.parse(sql);
        SqlSelect select = (SqlSelect) parsed;
        var tables = CalciteQueryPlanner.collectTableNames(select.getFrom());

        assertTrue(tables.contains("supplier"));
        assertTrue(tables.contains("lineitem"));
        assertTrue(tables.contains("nation"));
        assertEquals(3, tables.size());
    }

    @Test
    void plansQ07DerivedTableWithShardRewrite() throws Exception {
        String sql = loadQuery("Q07.sql");
        PlannedQuery planned = planner.plan(sql, tpchCatalog);

        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertTrue(planned.tableName().equals("lineitem") || planned.tableName().equals("orders"));
        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("lineitem_shard0") || fragmentSql.contains("orders_shard0"), fragmentSql);
        assertTrue(fragmentSql.contains("orders_shard0") || fragmentSql.contains("UNION ALL"), fragmentSql);
        assertFalse(fragmentSql.contains("No tables found"));
    }

    @Test
    void plansQ09DerivedTable() throws Exception {
        String sql = loadQuery("Q09.sql");
        PlannedQuery planned = planner.plan(sql, tpchCatalog);

        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertTrue(planned.tableName().equals("lineitem") || planned.tableName().equals("orders"));
        assertTrue(
                planned.fragments().get(0).sql().contains("lineitem_shard0")
                        || planned.fragments().get(0).sql().contains("orders_shard0"));
    }

    @Test
    void plansQ13NestedGroupByInDerivedTable() throws Exception {
        String sql = loadQuery("Q13.sql");
        PlannedQuery planned = planner.plan(sql, tpchCatalog);

        assertEquals(MergeStrategyType.NESTED_GROUP_BY_MERGE, planned.mergeStrategy());
        assertTrue(planned.hasNestedDerivedTable());
        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("customer") || fragmentSql.contains("orders"), fragmentSql);
        assertFalse(fragmentSql.contains("custdist"), fragmentSql);
    }

    private static String loadQuery(String name) throws Exception {
        Path path = Path.of("..", "benchmark", "src", "main", "resources", "queries", name)
                .normalize();
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
