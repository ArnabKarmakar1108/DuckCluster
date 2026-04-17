package io.duckcluster.coordinator.catalog;

import io.duckcluster.common.routing.ConsistentHashRing;
import io.duckcluster.proto.v1.ShardOwnership;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ShardCatalogCacheTest {

    private ShardCatalog catalog;

    @BeforeEach
    void setUp() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        catalog = new ShardCatalog(ring, 2);
        catalog.onWorkerAdded("worker-1");
        catalog.onWorkerAdded("worker-2");
        catalog.onWorkerAdded("worker-3");
    }

    @Test
    void registerCachedShards_tracksCacheState() {
        List<ShardOwnership> cached = List.of(
                ShardOwnership.newBuilder().setTableName("events").setShardId(0).build(),
                ShardOwnership.newBuilder().setTableName("events").setShardId(1).build());

        catalog.registerCachedShards("worker-3", cached);

        List<String> workers = catalog.getCachedWorkers("events", 0);
        assertTrue(workers.contains("worker-3"));

        List<String> workers1 = catalog.getCachedWorkers("events", 1);
        assertTrue(workers1.contains("worker-3"));

        List<String> workers2 = catalog.getCachedWorkers("events", 2);
        assertTrue(workers2.isEmpty());
    }

    @Test
    void registerCachedShards_fullReplace() {
        catalog.registerCachedShards("worker-3", List.of(
                ShardOwnership.newBuilder().setTableName("events").setShardId(0).build()));

        assertTrue(catalog.getCachedWorkers("events", 0).contains("worker-3"));

        catalog.registerCachedShards("worker-3", List.of(
                ShardOwnership.newBuilder().setTableName("events").setShardId(1).build()));

        assertFalse(catalog.getCachedWorkers("events", 0).contains("worker-3"));
        assertTrue(catalog.getCachedWorkers("events", 1).contains("worker-3"));
    }

    @Test
    void onWorkerRemoved_clearsCacheEntries() {
        catalog.registerCachedShards("worker-3", List.of(
                ShardOwnership.newBuilder().setTableName("events").setShardId(0).build()));

        catalog.onWorkerRemoved("worker-3");

        assertTrue(catalog.getCachedWorkers("events", 0).isEmpty());
    }

    @Test
    void getCachedWorkers_multipleWorkers() {
        catalog.registerCachedShards("worker-1", List.of(
                ShardOwnership.newBuilder().setTableName("events").setShardId(0).build()));
        catalog.registerCachedShards("worker-2", List.of(
                ShardOwnership.newBuilder().setTableName("events").setShardId(0).build()));

        List<String> workers = catalog.getCachedWorkers("events", 0);
        assertEquals(2, workers.size());
        assertTrue(workers.contains("worker-1"));
        assertTrue(workers.contains("worker-2"));
    }
}
