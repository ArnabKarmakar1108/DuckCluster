package io.duckcluster.coordinator.execution;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MergeSpoolHooksTest {

    @Test
    void forSpoolWritesRowsToDisk() throws Exception {
        try (QueryResultSpool spool = QueryResultSpool.open("hooks-test")) {
            MergeSpoolHooks.attach("q1", MergeSpoolHooks.forSpool(spool));
            MergeSpoolHooks.RowConsumer consumer = MergeSpoolHooks.get("q1").orElseThrow();
            consumer.setColumns(List.of("k", "v"));
            consumer.addRow(List.of("a", 1));
            consumer.addRow(List.of("b", 2));
            consumer.finish();
            MergeSpoolHooks.remove("q1");

            assertEquals(2, spool.rowCount());
            assertEquals(List.of("a", 1), spool.readPage(0, 1).get(0));
        }
    }

    @Test
    void removeDetachesConsumer() throws Exception {
        try (QueryResultSpool spool = QueryResultSpool.open("detach-test")) {
            MergeSpoolHooks.attach("gone", MergeSpoolHooks.forSpool(spool));
            MergeSpoolHooks.remove("gone");
            assertTrue(MergeSpoolHooks.get("gone").isEmpty());
        }
    }
}
