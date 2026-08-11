package io.duckcluster.common.planner;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BroadcastSqlNamesTest {
    @Test
    void tempTableWithoutQueryIdUsesLegacyPrefix() {
        assertEquals("__dc_bcast_orders", BroadcastSqlNames.tempTable("orders"));
    }

    @Test
    void tempTableWithQueryIdIsScoped() {
        assertEquals(
                "__dc_bcast_q-1_orders",
                BroadcastSqlNames.tempTable("q-1", "orders"));
    }
}
