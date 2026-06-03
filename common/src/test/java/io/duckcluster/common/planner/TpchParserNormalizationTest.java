package io.duckcluster.common.planner;

import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpchParserNormalizationTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final io.duckcluster.common.model.ClusterCatalog tpchCatalog = TpchCatalog.create();

    @Test
    void parsesQ11WithReservedValueAlias() throws Exception {
        PlannedQuery planned = planner.plan(loadQuery("Q11.sql"), tpchCatalog);
        assertEquals(MergeStrategyType.GROUP_BY_MERGE, planned.mergeStrategy());
        assertTrue(planned.analysis().outputColumnNames().contains("value"));
        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("GROUP BY"));
        assertTrue(fragmentSql.contains("HAVING"), fragmentSql);
        assertTrue(fragmentSql.contains("partsupp_shard0"), fragmentSql);
        String mergeSql = MergeSqlBuilder.buildGroupByMerge(planned.analysis(), planned.topK());
        assertTrue(mergeSql.contains("ORDER BY SUM(\"__dc_agg_0\") DESC"), mergeSql);
    }

    @Test
    void normalizesReservedAliasInOrderBy() {
        assertDoesNotThrow(() -> planner.parse("SELECT 1 AS value FROM events ORDER BY value"));
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
