package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TopKSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MergeSqlBuilderTest {

    @Test
    void buildsGroupByMergeSql() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of("category"),
                List.of(new AggregateSpec("cnt", "__dc_agg_0", AggregateFunction.COUNT, null)),
                List.of("category", "cnt"));

        String sql = MergeSqlBuilder.buildGroupByMerge(analysis);

        assertTrue(sql.contains("GROUP BY"));
        assertTrue(sql.contains("\"category\""));
        assertTrue(sql.contains("SUM(\"__dc_agg_0\")"));
    }

    @Test
    void buildsGroupByMergeSqlWithOrderBy() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of("c_count"),
                List.of(new AggregateSpec("custdist", "__dc_agg_0", AggregateFunction.COUNT, null)),
                List.of("c_count", "custdist"));
        TopKSpec topK = new TopKSpec(
                List.of(
                        new io.duckcluster.common.model.OrderByClause("custdist", true),
                        new io.duckcluster.common.model.OrderByClause("c_count", true)),
                -1);

        String sql = MergeSqlBuilder.buildGroupByMerge(analysis, topK);

        assertTrue(sql.contains("SUM(\"__dc_agg_0\") AS \"custdist\""));
        assertTrue(sql.contains("ORDER BY SUM(\"__dc_agg_0\") DESC, \"c_count\" DESC"));
    }

    @Test
    void buildsPartialAggregateMergeSql() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of(),
                List.of(
                        new AggregateSpec("count", "__dc_agg_0", AggregateFunction.COUNT, null),
                        new AggregateSpec("total", "__dc_agg_1", AggregateFunction.SUM, "value")),
                List.of("count", "total"));

        String sql = MergeSqlBuilder.buildPartialAggMerge(analysis);

        assertEquals(
                "SELECT SUM(\"__dc_agg_0\") AS \"count\", SUM(\"__dc_agg_1\") AS \"total\" FROM __merge_temp",
                sql);
    }

    @Test
    void buildsAvgMergeSqlFromSumAndCount() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of("category"),
                List.of(
                        new AggregateSpec(
                                "avg_score",
                                "__dc_agg_0_sum",
                                AggregateFunction.SUM,
                                "score",
                                AggregateSpec.AggregatePart.AVG_SUM),
                        new AggregateSpec(
                                "avg_score",
                                "__dc_agg_0_cnt",
                                AggregateFunction.COUNT,
                                "score",
                                AggregateSpec.AggregatePart.AVG_COUNT)),
                List.of("category", "avg_score"));

        String sql = MergeSqlBuilder.buildGroupByMerge(analysis);

        assertTrue(sql.contains("CAST(SUM(\"__dc_agg_0_sum\") AS DOUBLE) / NULLIF(SUM(\"__dc_agg_0_cnt\"), 0)"));
        assertTrue(sql.contains("AS \"avg_score\""));
    }

    @Test
    void buildsTopKMergeSql() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of(),
                List.of(),
                List.of("id", "value"));
        TopKSpec topK = new TopKSpec(List.of(new io.duckcluster.common.model.OrderByClause("value", true)), 5);

        String sql = MergeSqlBuilder.buildTopKMerge(analysis, topK);

        assertEquals(
                "SELECT \"id\", \"value\" FROM __merge_temp ORDER BY \"value\" DESC LIMIT 5",
                sql);
    }
}
