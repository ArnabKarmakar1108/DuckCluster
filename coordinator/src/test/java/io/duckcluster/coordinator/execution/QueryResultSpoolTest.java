package io.duckcluster.coordinator.execution;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryResultSpoolTest {

    @Test
    void paginatesWrittenRows() {
        try (QueryResultSpool spool = QueryResultSpool.open("test")) {
            spool.writeAll(List.of("a", "b"), List.of(List.of(1, "x"), List.of(2, "y"), List.of(3, "z")));
            assertEquals(3, spool.rowCount());
            assertEquals(List.of(List.of(1, "x")), spool.readPage(0, 1));
            assertEquals(List.of(List.of(2, "y"), List.of(3, "z")), spool.readPage(1, 10));
        }
    }
}
