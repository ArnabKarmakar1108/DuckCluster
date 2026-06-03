package io.duckcluster.common.planner;

import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpchArithmeticAggregateTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final io.duckcluster.common.model.ClusterCatalog tpchCatalog = TpchCatalog.create();

    @Test
    void plansQ14WithNestedAggregateMergeColumns() throws Exception {
        PlannedQuery planned = planner.plan(loadQuery("Q14.sql"), tpchCatalog);

        assertEquals(MergeStrategyType.PARTIAL_AGG, planned.mergeStrategy());
        assertEquals(2, planned.analysis().aggregates().size());
        assertEquals(1, planned.analysis().computedOutputs().size());
        assertEquals("promo_revenue", planned.analysis().computedOutputs().get(0).outputName());

        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("__dc_agg_0"), fragmentSql);
        assertTrue(fragmentSql.contains("__dc_agg_1"), fragmentSql);
        assertTrue(!fragmentSql.contains("100.00"), fragmentSql);

        String mergeSql = MergeSqlBuilder.buildPartialAggMerge(planned.analysis());
        assertTrue(mergeSql.contains("SUM(\"__dc_agg_0\")"), mergeSql);
        assertTrue(mergeSql.contains("SUM(\"__dc_agg_1\")"), mergeSql);
        assertTrue(mergeSql.contains("AS \"promo_revenue\""), mergeSql);
    }

    @Test
    void plansQ08DerivedTableWithArithmeticAggregates() throws Exception {
        PlannedQuery planned = planner.plan(loadQuery("Q08.sql"), tpchCatalog);

        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertEquals(2, planned.analysis().aggregates().size());
        assertEquals("mkt_share", planned.analysis().computedOutputs().get(0).outputName());

        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("GROUP BY"), fragmentSql);
        assertTrue(fragmentSql.contains("__dc_agg_0"), fragmentSql);
        assertTrue(fragmentSql.contains("__dc_agg_1"), fragmentSql);

        String mergeSql = MergeSqlBuilder.buildGroupByMerge(planned.analysis(), planned.topK());
        assertTrue(mergeSql.contains("SUM(\"__dc_agg_0\")"), mergeSql);
        assertTrue(mergeSql.contains("SUM(\"__dc_agg_1\")"), mergeSql);
        assertTrue(mergeSql.contains("AS \"mkt_share\""), mergeSql);
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
