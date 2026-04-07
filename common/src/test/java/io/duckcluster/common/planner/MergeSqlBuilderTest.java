package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.QueryAnalysis;
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
}
