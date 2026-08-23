package io.duckcluster.common.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterCatalogTest {

    @Test
    void declareAndRemoveTable() {
        ClusterCatalog catalog = new ClusterCatalog();
        catalog.declareTable(new TableShardConfig("events", 4, "user_id"));
        assertTrue(catalog.hasTable("events"));
        assertEquals(4, catalog.table("events").shardCount());
        assertEquals("user_id", catalog.table("events").shardKey());

        assertTrue(catalog.removeTable("events"));
        assertFalse(catalog.hasTable("events"));
        assertFalse(catalog.removeTable("missing"));
    }

    @Test
    void tracksActiveDataVersion() {
        ClusterCatalog catalog = new ClusterCatalog();
        catalog.declareTable(new TableShardConfig("events", 2, "id"));
        catalog.setActiveDataVersion("events", 3);
        assertEquals(3, catalog.activeDataVersion("events"));
    }
}
