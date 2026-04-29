package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryAnalysis;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryAnalysisExtractorTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();

    @Test
    void extractsGroupByAnalysis() {
        SqlSelect select = (SqlSelect) planner.parse("SELECT category, COUNT(*) AS cnt FROM events GROUP BY category");
        QueryAnalysis analysis = QueryAnalysisExtractor.extract(select, MergeStrategyType.GROUP_BY_MERGE);

        assertEquals(List.of("category"), analysis.groupByColumns());
        assertEquals(1, analysis.aggregates().size());
        assertEquals("cnt", analysis.aggregates().get(0).outputName());
        assertEquals(AggregateFunction.COUNT, analysis.aggregates().get(0).function());
    }

    @Test
    void extractsPartialAggregateAnalysis() {
        SqlSelect select = (SqlSelect) planner.parse("SELECT COUNT(*), SUM(id) FROM events");
        QueryAnalysis analysis = QueryAnalysisExtractor.extract(select, MergeStrategyType.PARTIAL_AGG);

        assertTrue(analysis.groupByColumns().isEmpty());
        assertEquals(2, analysis.aggregates().size());
        assertEquals(AggregateFunction.COUNT, analysis.aggregates().get(0).function());
        assertEquals(AggregateFunction.SUM, analysis.aggregates().get(1).function());
        assertEquals("id", analysis.aggregates().get(1).inputColumn());
    }

    @Test
    void extractsAvgAsSumAndCountParts() {
        SqlSelect select = (SqlSelect) planner.parse(
                "SELECT category, AVG(score) AS avg_score FROM events GROUP BY category");
        QueryAnalysis analysis = QueryAnalysisExtractor.extract(select, MergeStrategyType.GROUP_BY_MERGE);

        assertEquals(2, analysis.aggregates().size());
        assertEquals(AggregateSpec.AggregatePart.AVG_SUM, analysis.aggregates().get(0).part());
        assertEquals(AggregateFunction.SUM, analysis.aggregates().get(0).function());
        assertEquals(AggregateSpec.AggregatePart.AVG_COUNT, analysis.aggregates().get(1).part());
        assertEquals(AggregateFunction.COUNT, analysis.aggregates().get(1).function());
        assertEquals("avg_score", analysis.aggregates().get(0).outputName());
    }
}
