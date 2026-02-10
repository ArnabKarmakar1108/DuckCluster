package io.duckcluster.common.planner;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ShardPredicateInjectorTest {

    @Test
    void addsWhereClauseWhenMissing() {
        String sql = ShardPredicateInjector.inject("SELECT * FROM events", "id", 3, 1);
        assertEquals("SELECT * FROM events WHERE (id % 3) = 1", sql);
    }

    @Test
    void appendsPredicateToExistingWhereClause() {
        String sql = ShardPredicateInjector.inject("SELECT * FROM events WHERE id > 2", "id", 3, 0);
        assertEquals("SELECT * FROM events WHERE id > 2 AND (id % 3) = 0", sql);
    }
}
