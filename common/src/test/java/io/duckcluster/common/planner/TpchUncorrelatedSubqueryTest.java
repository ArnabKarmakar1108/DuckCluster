package io.duckcluster.common.planner;

import io.duckcluster.common.model.PlannedQuery;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpchUncorrelatedSubqueryTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final io.duckcluster.common.model.ClusterCatalog tpchCatalog = TpchCatalog.create();

    @Test
    void plansQ18WithSubqueryBroadcastPrefetch() throws Exception {
        PlannedQuery planned = planner.plan(loadQuery("Q18.sql"), tpchCatalog);
        assertEquals(1, planned.subqueryBroadcastTables().size());
        assertEquals("lineitem", planned.subqueryBroadcastTables().get(0).tableName());
        assertEquals(6, planned.subqueryBroadcastTables().get(0).shardCount());
    }

    @Test
    void plansQ18WithGlobalLineitemUnionInSubquery() throws Exception {
        PlannedQuery planned = planner.plan(loadQuery("Q18.sql"), tpchCatalog);
        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("UNION ALL"), fragmentSql);
        assertTrue(fragmentSql.contains("lineitem_shard0"), fragmentSql);
        assertTrue(fragmentSql.contains("lineitem_shard5"), fragmentSql);
    }

    @Test
    void plansQ16NotInStillShardQualifiesSupplier() throws Exception {
        PlannedQuery planned = planner.plan(loadQuery("Q16.sql"), tpchCatalog);
        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("supplier_shard0"), fragmentSql);
        assertTrue(!fragmentSql.contains("COUNT(DISTINCT"), fragmentSql);
    }

    @Test
    void plansQ20NestedUncorrelatedInRewritesInnerPartSubquery() throws Exception {
        PlannedQuery planned = planner.plan(loadQuery("Q20.sql"), tpchCatalog);
        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("part_shard0"), fragmentSql);
        assertTrue(fragmentSql.contains("partsupp_shard0"), fragmentSql);
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
