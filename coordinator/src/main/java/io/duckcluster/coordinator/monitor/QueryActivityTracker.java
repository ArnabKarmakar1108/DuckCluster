package io.duckcluster.coordinator.monitor;

import io.duckcluster.common.model.QueryResult;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class QueryActivityTracker {
    private static final int SQL_PREVIEW_LEN = 80;
    private static final int MAX_RECENT = 50;

    public enum Phase {
        PLAN,
        PREFETCH,
        FRAGMENTS,
        MERGE,
        DONE
    }

    public record ActiveQuery(
            String queryId,
            Phase phase,
            long startedAtEpochMs,
            String sqlPreview,
            int fragmentsTotal,
            int fragmentsDone) {

        public long elapsedMs() {
            return Math.max(0, System.currentTimeMillis() - startedAtEpochMs);
        }
    }

    public record RecentQuery(
            String queryId,
            long durationMs,
            String mergeStrategy,
            String status,
            String sqlPreview,
            long completedAtEpochMs) {}

    private static final class MutableActive {
        final String queryId;
        volatile Phase phase;
        final long startedAtEpochMs;
        final String sqlPreview;
        volatile int fragmentsTotal;
        volatile int fragmentsDone;

        MutableActive(String queryId, String sqlPreview) {
            this.queryId = queryId;
            this.phase = Phase.PLAN;
            this.startedAtEpochMs = System.currentTimeMillis();
            this.sqlPreview = sqlPreview;
        }

        ActiveQuery snapshot() {
            return new ActiveQuery(
                    queryId, phase, startedAtEpochMs, sqlPreview, fragmentsTotal, fragmentsDone);
        }
    }

    private final ConcurrentHashMap<String, MutableActive> active = new ConcurrentHashMap<>();
    private final Deque<RecentQuery> recent = new ArrayDeque<>();
    private final Object recentLock = new Object();

    public void start(String queryId, String sql) {
        active.put(queryId, new MutableActive(queryId, truncate(sql)));
    }

    public void setPhase(String queryId, Phase phase) {
        MutableActive entry = active.get(queryId);
        if (entry != null) {
            entry.phase = phase;
        }
    }

    public void setFragmentsTotal(String queryId, int total) {
        MutableActive entry = active.get(queryId);
        if (entry != null) {
            entry.fragmentsTotal = total;
        }
    }

    public void fragmentDone(String queryId) {
        MutableActive entry = active.get(queryId);
        if (entry != null) {
            entry.fragmentsDone++;
        }
    }

    public void complete(String queryId, QueryResult result) {
        MutableActive entry = active.remove(queryId);
        if (entry == null) {
            return;
        }
        RecentQuery record = new RecentQuery(
                queryId,
                result.stats().durationMs(),
                result.stats().mergeStrategy().name(),
                "OK",
                entry.sqlPreview,
                System.currentTimeMillis());
        appendRecent(record);
    }

    public void fail(String queryId, String error, long durationMs) {
        MutableActive entry = active.remove(queryId);
        String preview = entry != null ? entry.sqlPreview : "";
        RecentQuery record = new RecentQuery(
                queryId, durationMs, "—", "ERROR", preview, System.currentTimeMillis());
        appendRecent(record);
    }

    public List<ActiveQuery> activeQueries() {
        return active.values().stream().map(MutableActive::snapshot).toList();
    }

    public List<RecentQuery> recentQueries() {
        synchronized (recentLock) {
            return List.copyOf(recent);
        }
    }

    private void appendRecent(RecentQuery record) {
        synchronized (recentLock) {
            recent.addFirst(record);
            while (recent.size() > MAX_RECENT) {
                recent.removeLast();
            }
        }
    }

    private static String truncate(String sql) {
        if (sql == null) {
            return "";
        }
        String normalized = sql.strip().replaceAll("\\s+", " ");
        if (normalized.length() <= SQL_PREVIEW_LEN) {
            return normalized;
        }
        return normalized.substring(0, SQL_PREVIEW_LEN - 1) + "…";
    }
}
