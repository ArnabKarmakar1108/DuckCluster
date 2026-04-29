package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.model.TopKSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvgMergeStrategyTest {

    private final GroupByMergeStrategy groupByStrategy = new GroupByMergeStrategy();
    private final PartialAggMergeStrategy partialAggStrategy = new PartialAggMergeStrategy();

    @Test
    void mergesWeightedAvgAcrossGroupByFragments() {
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
        PlannedQuery plan = new PlannedQuery(
                "SELECT category, AVG(score) AS avg_score FROM events GROUP BY category",
                List.of("events"),
                List.of(),
                List.of(),
                MergeStrategyType.GROUP_BY_MERGE,
                analysis,
                List.of(),
                TopKSpec.none());
        MergeContext context = new MergeContext(
                "query-avg-1",
                plan,
                List.of(
                        new FragmentResult(
                                0,
                                0,
                                "worker-1",
                                3L,
                                List.of(new RowBatchData(
                                        List.of("category", "__dc_agg_0_sum", "__dc_agg_0_cnt"),
                                        List.of(List.of("A", "10", "1"))))),
                        new FragmentResult(
                                1,
                                1,
                                "worker-2",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("category", "__dc_agg_0_sum", "__dc_agg_0_cnt"),
                                        List.of(List.of("A", "20", "4")))))),
                5L);

        QueryResult result = groupByStrategy.merge(context);

        assertEquals("6.0", result.rows().stream()
                .filter(row -> "A".equals(row.get(0)))
                .findFirst()
                .orElseThrow()
                .get(1)
                .toString());
    }

    @Test
    void mergesWeightedAvgAcrossPartialAggFragments() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of(),
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
                List.of("avg_score"));
        PlannedQuery plan = new PlannedQuery(
                "SELECT AVG(score) FROM events",
                List.of("events"),
                List.of(),
                List.of(),
                MergeStrategyType.PARTIAL_AGG,
                analysis,
                List.of(),
                TopKSpec.none());
        MergeContext context = new MergeContext(
                "query-avg-2",
                plan,
                List.of(
                        new FragmentResult(
                                0,
                                0,
                                "worker-1",
                                3L,
                                List.of(new RowBatchData(
                                        List.of("__dc_agg_0_sum", "__dc_agg_0_cnt"),
                                        List.of(List.of("20", "2"))))),
                        new FragmentResult(
                                1,
                                1,
                                "worker-2",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("__dc_agg_0_sum", "__dc_agg_0_cnt"),
                                        List.of(List.of("30", "3")))))),
                5L);

        QueryResult result = partialAggStrategy.merge(context);

        assertEquals("10.0", result.rows().get(0).get(0).toString());
    }
}
