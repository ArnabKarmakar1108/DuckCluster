package io.duckcluster.coordinator.monitor;

import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.model.QueryResult.QueryStats;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryActivityTrackerTest {

    @Test
    void tracksActiveAndRecentQueries() {
        QueryActivityTracker tracker = new QueryActivityTracker();
        tracker.start("q1", "SELECT 1");
        tracker.setFragmentsTotal("q1", 3);
        tracker.setPhase("q1", QueryActivityTracker.Phase.FRAGMENTS);
        tracker.fragmentDone("q1");
        tracker.fragmentDone("q1");

        assertEquals(1, tracker.activeQueries().size());
        QueryActivityTracker.ActiveQuery active = tracker.activeQueries().get(0);
        assertEquals(QueryActivityTracker.Phase.FRAGMENTS, active.phase());
        assertEquals(2, active.fragmentsDone());
        assertEquals(3, active.fragmentsTotal());

        QueryResult result = new QueryResult(
                "q1",
                List.of("c"),
                List.of(List.of(1)),
                new QueryStats(MergeStrategyType.CONCATENATE, 1, 3, 42, Map.of()));
        tracker.complete("q1", result);

        assertTrue(tracker.activeQueries().isEmpty());
        assertEquals(1, tracker.recentQueries().size());
        assertEquals("OK", tracker.recentQueries().get(0).status());
        assertEquals(42, tracker.recentQueries().get(0).durationMs());
    }

    @Test
    void truncatesLongSqlPreview() {
        QueryActivityTracker tracker = new QueryActivityTracker();
        String longSql = "SELECT " + "x".repeat(120);
        tracker.start("q2", longSql);

        String preview = tracker.activeQueries().get(0).sqlPreview();
        assertTrue(preview.length() <= 80);
        assertTrue(preview.endsWith("…"));
    }
}
