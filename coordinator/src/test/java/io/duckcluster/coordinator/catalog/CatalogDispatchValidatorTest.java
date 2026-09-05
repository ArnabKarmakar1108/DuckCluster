package io.duckcluster.coordinator.catalog;

import io.duckcluster.common.model.BroadcastTable;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TableShardConfig;
import io.duckcluster.common.routing.ConsistentHashRing;
import io.duckcluster.coordinator.http.ApiErrorCode;
import io.duckcluster.coordinator.http.ApiException;
import io.duckcluster.proto.v1.ShardOwnership;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CatalogDispatchValidatorTest {

    private ClusterCatalog catalog;
    private ShardCatalog shardCatalog;
    private CatalogDispatchValidator validator;

    @BeforeEach
    void setUp() {
        catalog = new ClusterCatalog();
        catalog.declareTable(new TableShardConfig("events", 2, "id"));
        shardCatalog = new ShardCatalog(new ConsistentHashRing(128), 2, catalog);
        shardCatalog.onWorkerAdded("worker-1");
        shardCatalog.registerOwnership(
                "worker-1",
                List.of(ShardOwnership.newBuilder().setTableName("events").setShardId(0).build()));
        validator = new CatalogDispatchValidator(catalog, shardCatalog);
    }

    @Test
    void rejectsUnknownTable() {
        PlannedQuery plan = planned("lineitem");
        ApiException error = assertThrows(ApiException.class, () -> validator.validate(plan));
        assertEquals(ApiErrorCode.PLANNING_ERROR, error.code());
    }

    @Test
    void rejectsMissingShardOwner() {
        PlannedQuery plan = planned("events");
        ApiException error = assertThrows(ApiException.class, () -> validator.validate(plan));
        assertEquals(ApiErrorCode.NO_WORKERS, error.code());
    }

    @Test
    void acceptsWhenAllShardsOwned() {
        shardCatalog.registerOwnership(
                "worker-1",
                List.of(
                        ShardOwnership.newBuilder().setTableName("events").setShardId(0).build(),
                        ShardOwnership.newBuilder().setTableName("events").setShardId(1).build()));
        catalog.declareTable(new TableShardConfig("events", 2, "id"));
        assertDoesNotThrow(() -> validator.validate(planned("events")));
    }

    private static PlannedQuery planned(String table) {
        return new PlannedQuery(
                "SELECT * FROM " + table,
                List.of(table),
                List.of(),
                List.of(new FragmentSpec(0, 0, "SELECT 1", MergeStrategyType.CONCATENATE)),
                MergeStrategyType.CONCATENATE,
                QueryAnalysis.empty(),
                List.of(),
                null);
    }
}
