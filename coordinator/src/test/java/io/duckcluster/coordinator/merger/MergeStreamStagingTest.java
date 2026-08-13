package io.duckcluster.coordinator.merger;

import io.duckcluster.common.merger.FragmentResult;
import io.duckcluster.common.merger.MergeContext;
import io.duckcluster.common.merger.RowBatchData;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.TableShardConfig;
import io.duckcluster.common.planner.CalciteQueryPlanner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MergeStreamStagingTest {

    @AfterEach
    void tearDownStaging() {
        MergeStreamRegistry.remove("stream-test");
    }

    @Test
    void streamsBatchesIntoMergeTempTable() throws Exception {
        ClusterCatalog catalog = ClusterCatalog.withTables(Map.of("events", new TableShardConfig("events", 2, "id")));
        CalciteQueryPlanner planner = new CalciteQueryPlanner();
        PlannedQuery plan = planner.plan("SELECT category, COUNT(*) AS cnt FROM events GROUP BY category", catalog);
        CoordinatorMergeRuntime.initialize(2, 512, Path.of(System.getProperty("java.io.tmpdir"), "merge-stream-test"));

        assertTrue(MergeStreamRegistry.tryStart("stream-test", plan));
        MergeStreamRegistry.appendBatch(
                "stream-test",
                0,
                new RowBatchData(
                        List.of("category", "__dc_agg_0"),
                        List.of(List.of("a", "1"), List.of("b", "2"))));
        MergeStreamRegistry.appendBatch(
                "stream-test",
                1,
                new RowBatchData(List.of("category", "__dc_agg_0"), List.of(List.of("a", "3"))));

        MergeContext context = new MergeContext(
                "stream-test",
                plan,
                List.of(
                        new FragmentResult(0, 0, "w0", 1L, List.of()),
                        new FragmentResult(1, 1, "w1", 1L, List.of())),
                5L);

        MergeStreamStaging staging = MergeStreamRegistry.get("stream-test");
        Connection connection = staging.connection();
        staging.assembleInto(connection, "__merge_temp", context);
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(
                        "SELECT category, SUM(__dc_agg_0) AS total FROM __merge_temp "
                                + "GROUP BY category ORDER BY category")) {
                assertTrue(resultSet.next());
                assertEquals("a", resultSet.getString(1));
                assertEquals(4L, resultSet.getLong(2));
                assertTrue(resultSet.next());
                assertEquals("b", resultSet.getString(1));
                assertEquals(2L, resultSet.getLong(2));
        }
    }
}
