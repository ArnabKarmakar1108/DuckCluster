package io.duckcluster.common.planner;

import io.duckcluster.common.model.PlannedQuery;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TpchTopKPhase8Test {
    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final io.duckcluster.common.model.ClusterCatalog tpch = TpchCatalog.create();

    @Test
    void q03MergeOrdersByAggregateExpression() throws Exception {
        PlannedQuery planned = planner.plan(load("q03.sql"), tpch);
        String mergeSql = MergeSqlBuilder.buildGroupByMerge(planned.analysis(), planned.topK());
        assertTrue(mergeSql.contains("ORDER BY SUM(\"__dc_agg_0\") DESC"));
        assertTrue(mergeSql.contains("SUM(\"__dc_agg_0\") AS \"revenue\""));
    }

    @Test
    void q16OrderByDistinctCountAlias() throws Exception {
        PlannedQuery planned = planner.plan(load("q16.sql"), tpch);
        String mergeSql = MergeSqlBuilder.buildGroupByMerge(planned.analysis(), planned.topK());
        assertTrue(mergeSql.contains("ORDER BY COUNT(DISTINCT \"__dc_distinct_0\") DESC"));
        assertTrue(mergeSql.contains("COUNT(DISTINCT"));
    }

    @Test
    void q18GroupByMergeWithTopKOnNonAggregateColumns() throws Exception {
        PlannedQuery planned = planner.plan(load("q18.sql"), tpch);
        String mergeSql = MergeSqlBuilder.buildGroupByMerge(planned.analysis(), planned.topK());
        assertTrue(mergeSql.contains("GROUP BY"));
        assertTrue(mergeSql.contains("ORDER BY \"o_totalprice\" DESC"));
        assertTrue(mergeSql.contains("LIMIT 100"));
    }

    @Test
    void q19PlansPartialAgg() throws Exception {
        PlannedQuery planned = planner.plan(load("q19.sql"), tpch);
        String mergeSql = MergeSqlBuilder.buildPartialAggMerge(planned.analysis());
        assertTrue(mergeSql.contains("SUM(\"__dc_agg_0\") AS \"revenue\""));
    }

    private String load(String name) throws Exception {
        Path path = Path.of("..", "tests", "integration", "queries", "tpch", name);
        String sql = Files.readString(path);
        return sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql;
    }
}
