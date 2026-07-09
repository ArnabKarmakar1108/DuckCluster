package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.OrderByClause;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.model.TopKSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TopKMergeStrategyTest {

    private final TopKMergeStrategy strategy = new TopKMergeStrategy();

    @Test
    void mergesTopRowsAcrossFragments() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of(),
                List.of(),
                List.of("id", "value"));
        PlannedQuery plan = new PlannedQuery(
                "SELECT id, value FROM events ORDER BY value DESC LIMIT 2",
                List.of("events"),
                List.of(),
                List.of(),
                MergeStrategyType.TOP_K,
                analysis,
                List.of(),
                new TopKSpec(List.of(new OrderByClause("value", true)), 2));
        MergeContext context = new MergeContext(
                "query-1",
                plan,
                List.of(
                        new FragmentResult(
                                0,
                                0,
                                "worker-1",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("id", "value"),
                                        List.of(List.of("1", "50"), List.of("2", "30"))))),
                        new FragmentResult(
                                1,
                                1,
                                "worker-2",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("id", "value"),
                                        List.of(List.of("3", "40"), List.of("4", "10")))))),
                4L);

        QueryResult result = strategy.merge(context);

        assertEquals(List.of("id", "value"), result.columns());
        assertEquals(2, result.rows().size());
        assertEquals("1", result.rows().get(0).get(0).toString());
        assertEquals("50", result.rows().get(0).get(1).toString());
        assertEquals("3", result.rows().get(1).get(0).toString());
        assertEquals("40", result.rows().get(1).get(1).toString());
    }

    @Test
    void mergesTopRowsWithThreeDigitScores() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of(),
                List.of(),
                List.of("id", "score"));
        PlannedQuery plan = new PlannedQuery(
                "SELECT id, score FROM events ORDER BY score DESC LIMIT 3",
                List.of("events"),
                List.of(),
                List.of(),
                MergeStrategyType.TOP_K,
                analysis,
                List.of(),
                new TopKSpec(List.of(new OrderByClause("score", true)), 3));
        MergeContext context = new MergeContext(
                "query-3",
                plan,
                List.of(
                        new FragmentResult(
                                0,
                                0,
                                "worker-1",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("id", "score"),
                                        List.of(
                                                List.of("7", "70"),
                                                List.of("8", "80"),
                                                List.of("9", "90"))))),
                        new FragmentResult(
                                1,
                                1,
                                "worker-2",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("id", "score"),
                                        List.of(List.of("10", "100")))))),
                4L);

        QueryResult result = strategy.merge(context);

        assertEquals(List.of("id", "score"), result.columns());
        assertEquals(3, result.rows().size());
        assertEquals("10", result.rows().get(0).get(0).toString());
        assertEquals("100", result.rows().get(0).get(1).toString());
        assertEquals("9", result.rows().get(1).get(0).toString());
        assertEquals("90", result.rows().get(1).get(1).toString());
        assertEquals("8", result.rows().get(2).get(0).toString());
        assertEquals("80", result.rows().get(2).get(1).toString());
    }

    @Test
    void mergesTopRowsWithStringOrderByColumns() {
        QueryAnalysis analysis = new QueryAnalysis(
                List.of(),
                List.of(),
                List.of("s_acctbal", "s_name"));
        PlannedQuery plan = new PlannedQuery(
                "SELECT s_acctbal, s_name FROM supplier ORDER BY s_acctbal DESC, s_name LIMIT 2",
                List.of("supplier"),
                List.of(),
                List.of(),
                MergeStrategyType.TOP_K,
                analysis,
                List.of(),
                new TopKSpec(
                        List.of(
                                new OrderByClause("s_acctbal", true),
                                new OrderByClause("s_name", false)),
                        2));
        MergeContext context = new MergeContext(
                "query-4",
                plan,
                List.of(
                        new FragmentResult(
                                0,
                                0,
                                "worker-1",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("s_acctbal", "s_name"),
                                        List.of(
                                                List.of("100.5", "Supplier#000000010"),
                                                List.of("90.0", "Supplier#000000020"))))),
                        new FragmentResult(
                                1,
                                1,
                                "worker-2",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("s_acctbal", "s_name"),
                                        List.of(
                                                List.of("95.0", "Supplier#000000077"),
                                                List.of("80.0", "Supplier#000000013")))))),
                4L);

        QueryResult result = strategy.merge(context);

        assertEquals(List.of("s_acctbal", "s_name"), result.columns());
        assertEquals(2, result.rows().size());
        assertEquals("100.5", result.rows().get(0).get(0).toString());
        assertEquals("Supplier#000000010", result.rows().get(0).get(1).toString());
        assertEquals("95.0", result.rows().get(1).get(0).toString());
        assertEquals("Supplier#000000077", result.rows().get(1).get(1).toString());
    }

    @Test
    void mergesSelectStarLimitUsingFragmentColumnNames() {
        QueryAnalysis analysis = new QueryAnalysis(List.of(), List.of(), List.of("*"));
        PlannedQuery plan = new PlannedQuery(
                "SELECT * FROM customer LIMIT 3",
                List.of("customer"),
                List.of(),
                List.of(),
                MergeStrategyType.TOP_K,
                analysis,
                List.of(),
                new TopKSpec(List.of(), 3));
        MergeContext context = new MergeContext(
                "query-2",
                plan,
                List.of(
                        new FragmentResult(
                                0,
                                0,
                                "worker-1",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("c_custkey", "c_name"),
                                        List.of(List.of("1", "Alice"), List.of("2", "Bob"))))),
                        new FragmentResult(
                                1,
                                1,
                                "worker-2",
                                2L,
                                List.of(new RowBatchData(
                                        List.of("c_custkey", "c_name"),
                                        List.of(List.of("3", "Carol")))))),
                4L);

        QueryResult result = strategy.merge(context);

        assertEquals(List.of("c_custkey", "c_name"), result.columns());
        assertEquals(3, result.rows().size());
    }
}
