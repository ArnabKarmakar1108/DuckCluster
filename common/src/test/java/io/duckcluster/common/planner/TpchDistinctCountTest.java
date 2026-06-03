package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpchDistinctCountTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final io.duckcluster.common.model.ClusterCatalog tpchCatalog = TpchCatalog.create();

    @Test
    void plansQ16WithDistinctCountMerge() throws Exception {
        PlannedQuery planned = planner.plan(loadQuery("Q16.sql"), tpchCatalog);

        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertEquals(1, planned.analysis().aggregates().size());
        assertEquals(AggregateSpec.AggregatePart.DISTINCT_COUNT, planned.analysis().aggregates().get(0).part());
        assertEquals("__dc_distinct_0", planned.analysis().aggregates().get(0).mergeColumnName());

        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("\"ps_suppkey\" AS \"__dc_distinct_0\""), fragmentSql);
        assertTrue(fragmentSql.contains("supplier_shard0"), fragmentSql);
        assertTrue(!fragmentSql.contains("COUNT(DISTINCT"), fragmentSql);
        assertTrue(fragmentSql.contains("GROUP BY"), fragmentSql);
        assertTrue(fragmentSql.contains("\"ps_suppkey\""), fragmentSql);

        String mergeSql = MergeSqlBuilder.buildGroupByMerge(planned.analysis(), planned.topK());
        assertTrue(mergeSql.contains("COUNT(DISTINCT \"__dc_distinct_0\")"), mergeSql);
        assertTrue(mergeSql.contains("AS \"supplier_cnt\""), mergeSql);
        assertTrue(!mergeSql.contains("SUM(\"__dc_distinct_0\")"), mergeSql);
    }

    @Test
    void plansSimpleDistinctCount() {
        PlannedQuery planned = planner.plan(
                "SELECT region, COUNT(DISTINCT nation) AS n FROM t GROUP BY region",
                ClusterCatalog.withTables(java.util.Map.of(
                        "t", new io.duckcluster.common.model.TableShardConfig("t", 2, "id"))));

        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertEquals(AggregateSpec.AggregatePart.DISTINCT_COUNT, planned.analysis().aggregates().get(0).part());

        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("\"nation\" AS \"__dc_distinct_0\""), fragmentSql);

        String mergeSql = MergeSqlBuilder.buildGroupByMerge(planned.analysis());
        assertTrue(mergeSql.contains("COUNT(DISTINCT \"__dc_distinct_0\")"), mergeSql);
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
