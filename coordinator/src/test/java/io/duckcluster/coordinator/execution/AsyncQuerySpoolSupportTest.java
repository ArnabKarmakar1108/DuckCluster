package io.duckcluster.coordinator.execution;

import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncQuerySpoolSupportTest {

    @Test
    void copiesMergedRowsIntoSpoolWhenPresent() throws Exception {
        QueryResult merged = new QueryResult(
                "q1",
                List.of("a"),
                List.of(List.of(1)),
                new QueryResult.QueryStats(MergeStrategyType.CONCATENATE, 1, 1, 1, Map.of()));
        try (QueryResultSpool spool = QueryResultSpool.open("spool-copy")) {
            QueryResult client = AsyncQuerySpoolSupport.toClientResult(spool, merged);
            assertTrue(client.rows().isEmpty());
            assertEquals(1, spool.rowCount());
        }
    }
}
