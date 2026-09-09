package io.duckcluster.coordinator.merger;

import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.OrderByClause;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TopKSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TopKMergeTypingTest {

    @Test
    void plannedTopKOrderByColumnUsesNumericType() {
        QueryAnalysis analysis = new QueryAnalysis(List.of(), List.of(), List.of("id", "score"));
        TopKSpec topK = new TopKSpec(List.of(new OrderByClause("score", true)), 3);
        PlannedQuery plan = new PlannedQuery(
                "SELECT id, score FROM events ORDER BY score DESC LIMIT 3",
                List.of("events"),
                List.of(),
                List.of(),
                MergeStrategyType.TOP_K,
                analysis,
                List.of(),
                topK);

        List<String> types = DuckDbMergeSupport.columnTypesForPlannedQuery(plan, List.of("id", "score"));
        assertEquals("VARCHAR", types.get(0));
        assertEquals("DOUBLE", types.get(1));
    }
}
