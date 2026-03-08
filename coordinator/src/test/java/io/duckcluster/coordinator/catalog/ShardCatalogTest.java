package io.duckcluster.coordinator.catalog;

import io.duckcluster.common.routing.ConsistentHashRing;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ShardCatalogTest {

    @Test
    void getOwners_returnsReplicationFactorWorkers() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        ShardCatalog catalog = new ShardCatalog(ring, 2);
        catalog.onWorkerAdded("worker-0");
        catalog.onWorkerAdded("worker-1");
        catalog.onWorkerAdded("worker-2");

        List<String> owners = catalog.getOwners("events", 0);
        assertEquals(2, owners.size());
        assertNotEquals(owners.get(0), owners.get(1));
    }

    @Test
    void onWorkerRemoved_updatesRouting() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        ShardCatalog catalog = new ShardCatalog(ring, 2);
        catalog.onWorkerAdded("worker-0");
        catalog.onWorkerAdded("worker-1");
        catalog.onWorkerAdded("worker-2");

        List<String> before = catalog.getOwners("events", 0);
        String primary = before.get(0);

        catalog.onWorkerRemoved(primary);

        List<String> after = catalog.getOwners("events", 0);
        assertEquals(2, after.size());
        assertFalse(after.contains(primary));
    }

    @Test
    void getOwners_withSingleWorker_returnsSingleElement() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        ShardCatalog catalog = new ShardCatalog(ring, 2);
        catalog.onWorkerAdded("worker-0");

        List<String> owners = catalog.getOwners("events", 0);
        assertEquals(1, owners.size());
        assertEquals("worker-0", owners.get(0));
    }

    @Test
    void getWorkerCount_tracksAdditionsAndRemovals() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        ShardCatalog catalog = new ShardCatalog(ring, 2);

        assertEquals(0, catalog.getWorkerCount());
        catalog.onWorkerAdded("worker-0");
        assertEquals(1, catalog.getWorkerCount());
        catalog.onWorkerAdded("worker-1");
        assertEquals(2, catalog.getWorkerCount());
        catalog.onWorkerRemoved("worker-0");
        assertEquals(1, catalog.getWorkerCount());
    }
}
