package io.duckcluster.common.planner;

import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpchWithCteTest {
    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final io.duckcluster.common.model.ClusterCatalog tpch = TpchCatalog.create();

    @Test
    void plansQ15WithCteMerge() throws Exception {
        PlannedQuery planned = planner.plan(load("q15.sql"), tpch);
        assertEquals(MergeStrategyType.WITH_CTE_MERGE, planned.mergeStrategy());
        assertTrue(planned.hasWithCte());
        assertEquals("lineitem", planned.tableName());
        assertEquals(6, planned.fragments().size());
        assertEquals(1, planned.withCte().coordinatorDimensionTables().size());
        assertEquals("supplier", planned.withCte().coordinatorDimensionTables().get(0));
        assertTrue(planned.withCte().outerSql().contains("__merge_temp"));
    }

    @Test
    void q20CorrelatedScalarUsesGlobalLineitemUnion() throws Exception {
        PlannedQuery planned = planner.plan(load("q20.sql"), tpch);
        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("UNION ALL"));
        assertTrue(planned.subqueryBroadcastTables().stream()
                .anyMatch(table -> table.tableName().equals("lineitem")));
    }

    @Test
    void q22RewritesDerivedTableSubqueries() throws Exception {
        PlannedQuery planned = planner.plan(load("q22.sql"), tpch);
        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("customer_shard0"));
        assertTrue(fragmentSql.contains("UNION ALL"));
        assertTrue(planned.subqueryBroadcastTables().stream()
                .anyMatch(table -> table.tableName().equals("orders")));
    }

    @Test
    void q02TopKMergePreservesStringOrderBy() throws Exception {
        PlannedQuery planned = planner.plan(load("q02.sql"), tpch);
        String mergeSql = MergeSqlBuilder.buildTopKMerge(planned.analysis(), planned.topK());
        assertFalse(mergeSql.contains("CAST(\"n_name\" AS DOUBLE)"));
        assertTrue(mergeSql.contains("ORDER BY \"s_acctbal\" DESC, \"n_name\""));
    }

    private String load(String name) throws Exception {
        Path path = Path.of("..", "tests", "integration", "queries", "tpch", name).normalize();
        if (!Files.exists(path)) {
            path = Path.of("tests", "integration", "queries", "tpch", name);
        }
        String sql = Files.readString(path);
        return sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql;
    }
}
