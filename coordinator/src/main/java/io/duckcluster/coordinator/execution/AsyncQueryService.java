package io.duckcluster.coordinator.execution;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.coordinator.monitor.QueryActivityTracker;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class AsyncQueryService {
    public static final int DEFAULT_RESULT_LIMIT = 1_000;
    public static final int MAX_RESULT_LIMIT = 10_000;

    public record StatusView(
            String queryId,
            AsyncQueryJob.Status status,
            QueryActivityTracker.Phase phase,
            int fragmentsDone,
            int fragmentsTotal,
            long elapsedMs,
            String error,
            QueryResult.QueryStats stats) {}

    public record ResultChunk(
            QueryResult result,
            int offset,
            int limit,
            List<List<Object>> rows,
            Integer nextOffset,
            long totalRowCount) {}

    private final QueryExecutionService queryExecutionService;
    private final QueryActivityTracker activityTracker;
    private final long resultTtlMs;
    private final int maxStoredJobs;
    private final long maxStoredBytes;
    private final ConcurrentHashMap<String, AsyncQueryJobEntry> jobs = new ConcurrentHashMap<>();
    private final AtomicLong totalRetainedBytes = new AtomicLong();
    private final ExecutorService executor;
    private final ScheduledExecutorService sweeper;
    private final QueryAdmissionControl admission;

    public AsyncQueryService(
            QueryExecutionService queryExecutionService,
            QueryActivityTracker activityTracker,
            ClusterConfig config,
            QueryAdmissionControl admission) {
        this.queryExecutionService = queryExecutionService;
        this.activityTracker = activityTracker;
        this.admission = admission;
        this.resultTtlMs = config.asyncResultTtlMs();
        this.maxStoredJobs = config.asyncMaxStoredJobs();
        this.maxStoredBytes = config.asyncMaxStoredBytes();
        this.executor = Executors.newFixedThreadPool(
                Math.max(1, config.maxAsyncQueryThreads()),
                r -> {
                    Thread thread = new Thread(r, "async-query");
                    thread.setDaemon(true);
                    return thread;
                });
        this.sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "async-query-sweeper");
            thread.setDaemon(true);
            return thread;
        });
        long sweepMs = Math.max(30_000L, config.asyncResultTtlMs() / 4);
        sweeper.scheduleAtFixedRate(this::purgeExpiredAndOverCapacity, sweepMs, sweepMs, TimeUnit.MILLISECONDS);
    }

    public Optional<String> trySubmit(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("sql is required");
        }
        if (!admission.tryAcquire()) {
            return Optional.empty();
        }
        purgeExpiredAndOverCapacity();
        String queryId = UUID.randomUUID().toString();
        AsyncQueryJob job = new AsyncQueryJob();
        QueryResultSpool spool = QueryResultSpool.open(queryId);
        jobs.put(queryId, new AsyncQueryJobEntry(job, spool, System.currentTimeMillis()));
        executor.submit(() -> runQuery(queryId, sql.strip(), job));
        return Optional.of(queryId);
    }

    public String submit(String sql) {
        return trySubmit(sql).orElseThrow(() -> new QueryRejectedException("too many concurrent queries"));
    }

    public boolean cancel(String queryId) {
        AsyncQueryJobEntry entry = jobs.get(queryId);
        if (entry == null) {
            return false;
        }
        AsyncQueryJob job = entry.job;
        if (isTerminal(job.status()) || job.status() == AsyncQueryJob.Status.CANCELLED) {
            return false;
        }
        boolean wasQueued = job.status() == AsyncQueryJob.Status.QUEUED;
        job.cancel("cancelled by client");
        queryExecutionService.cancelQuery(queryId);
        if (wasQueued) {
            admission.release();
            markCompleted(queryId, null);
        }
        return true;
    }

    public Optional<StatusView> status(String queryId) {
        purgeExpiredAndOverCapacity();
        AsyncQueryJobEntry entry = jobs.get(queryId);
        if (entry == null) {
            return Optional.empty();
        }
        return Optional.of(buildStatusView(queryId, entry.job));
    }

    public ResultChunk resultChunk(String queryId, int offset, int limit) {
        purgeExpiredAndOverCapacity();
        AsyncQueryJobEntry entry = jobs.get(queryId);
        if (entry == null) {
            throw new IllegalArgumentException("unknown query id: " + queryId);
        }
        AsyncQueryJob job = entry.job;
        if (job.status() == AsyncQueryJob.Status.EXPIRED) {
            throw new AsyncQueryExpiredException(queryId);
        }
        if (job.status() == AsyncQueryJob.Status.FAILED) {
            throw new IllegalStateException(job.errorMessage().orElse("query failed"));
        }
        boolean partialConcat =
                job.status() == AsyncQueryJob.Status.RUNNING && entry.spool != null && entry.spool.hasColumns();
        if (job.status() != AsyncQueryJob.Status.SUCCEEDED && !partialConcat) {
            throw new IllegalStateException("query is not finished");
        }
        QueryResult result = job.result().orElseGet(() -> partialResultShell(queryId, entry));
        int safeOffset = Math.max(0, offset);
        int safeLimit = Math.min(Math.max(1, limit), MAX_RESULT_LIMIT);
        if (entry.spool != null && (result.rows().isEmpty() || partialConcat)) {
            long totalRows = entry.spool.rowCount();
            List<List<Object>> page = entry.spool.readPage(safeOffset, safeLimit);
            int next = safeOffset + page.size();
            Integer nextOffset = next < totalRows ? next : null;
            return new ResultChunk(result, safeOffset, safeLimit, page, nextOffset, totalRows);
        }
        List<List<Object>> allRows = result.rows();
        int from = Math.min(safeOffset, allRows.size());
        int to = Math.min(from + safeLimit, allRows.size());
        List<List<Object>> page = new ArrayList<>(allRows.subList(from, to));
        Integer nextOffset = to < allRows.size() ? to : null;
        return new ResultChunk(result, safeOffset, safeLimit, page, nextOffset, allRows.size());
    }

    private void runQuery(String queryId, String sql, AsyncQueryJob job) {
        if (job.status() == AsyncQueryJob.Status.CANCELLED) {
            admission.release();
            markCompleted(queryId, null);
            return;
        }
        job.markRunning();
        if (job.status() == AsyncQueryJob.Status.CANCELLED) {
            admission.release();
            markCompleted(queryId, null);
            return;
        }
        AsyncQueryJobEntry entry = jobs.get(queryId);
        QueryResultSpool spool = entry != null ? entry.spool : null;
        try {
            QueryResult result = queryExecutionService.execute(queryId, sql, false, spool);
            if (job.status() == AsyncQueryJob.Status.CANCELLED) {
                markCompleted(queryId, null);
                return;
            }
            job.complete(result);
            markCompleted(queryId, result);
        } catch (QueryCancelledException cancelled) {
            if (job.status() != AsyncQueryJob.Status.CANCELLED) {
                job.cancel(cancelled.getMessage());
            }
            markCompleted(queryId, null);
        } catch (RuntimeException exception) {
            if (job.status() == AsyncQueryJob.Status.CANCELLED) {
                markCompleted(queryId, null);
                return;
            }
            job.fail(exception.getMessage());
            markCompleted(queryId, null);
        } finally {
            admission.release();
        }
    }

    private void markCompleted(String queryId, QueryResult result) {
        AsyncQueryJobEntry entry = jobs.get(queryId);
        if (entry == null) {
            return;
        }
        entry.completedAtEpochMs = System.currentTimeMillis();
        if (result != null) {
            long spoolBytes = entry.spool != null ? entry.spool.retainedBytes() : 0L;
            entry.retainedBytes = QueryResultSizeEstimator.estimateBytes(result, spoolBytes);
            totalRetainedBytes.addAndGet(entry.retainedBytes);
        }
    }

    private static QueryResult partialResultShell(String queryId, AsyncQueryJobEntry entry) {
        return new QueryResult(queryId, entry.spool.columns(), List.of(), null);
    }

    private StatusView buildStatusView(String queryId, AsyncQueryJob job) {
        Optional<QueryActivityTracker.ActiveQuery> active = activityTracker.activeQueries().stream()
                .filter(entry -> entry.queryId().equals(queryId))
                .findFirst();

        QueryActivityTracker.Phase phase = active.map(QueryActivityTracker.ActiveQuery::phase).orElse(null);
        int fragmentsDone = active.map(QueryActivityTracker.ActiveQuery::fragmentsDone).orElse(0);
        int fragmentsTotal = active.map(QueryActivityTracker.ActiveQuery::fragmentsTotal).orElse(0);
        long elapsedMs = active.map(QueryActivityTracker.ActiveQuery::elapsedMs).orElse(0L);

        QueryResult.QueryStats stats = job.result().map(QueryResult::stats).orElse(null);
        String error = job.errorMessage().orElse(null);
        return new StatusView(queryId, job.status(), phase, fragmentsDone, fragmentsTotal, elapsedMs, error, stats);
    }

    private void purgeExpiredAndOverCapacity() {
        long now = System.currentTimeMillis();
        expireByTtl(now);
        expireByCapacity(now);
    }

    private void expireByTtl(long now) {
        for (Map.Entry<String, AsyncQueryJobEntry> entry : jobs.entrySet()) {
            AsyncQueryJobEntry jobEntry = entry.getValue();
            if (!isTerminal(jobEntry.job.status())) {
                continue;
            }
            if (jobEntry.completedAtEpochMs < 0) {
                continue;
            }
            if (now - jobEntry.completedAtEpochMs <= resultTtlMs) {
                continue;
            }
            expireEntry(entry.getKey(), jobEntry);
        }
    }

    private void expireByCapacity(long now) {
        while (jobs.size() > maxStoredJobs || totalRetainedBytes.get() > maxStoredBytes) {
            Optional<Map.Entry<String, AsyncQueryJobEntry>> oldest = jobs.entrySet().stream()
                    .filter(entry -> isTerminal(entry.getValue().job.status()))
                    .filter(entry -> entry.getValue().completedAtEpochMs > 0)
                    .min(Comparator.comparingLong(entry -> entry.getValue().completedAtEpochMs));
            if (oldest.isEmpty()) {
                break;
            }
            String queryId = oldest.get().getKey();
            AsyncQueryJobEntry entry = oldest.get().getValue();
            if (entry.job.status() == AsyncQueryJob.Status.EXPIRED) {
                jobs.remove(queryId);
                continue;
            }
            evictForCapacity(queryId, entry);
        }
        // Drop long-expired tombstones so unknown vs expired stays meaningful for a while only.
        jobs.entrySet().removeIf(entry -> {
            AsyncQueryJobEntry jobEntry = entry.getValue();
            if (jobEntry.job.status() != AsyncQueryJob.Status.EXPIRED) {
                return false;
            }
            return jobEntry.completedAtEpochMs > 0
                    && now - jobEntry.completedAtEpochMs > resultTtlMs * 2;
        });
    }

    private void evictForCapacity(String queryId, AsyncQueryJobEntry entry) {
        if (entry == null) {
            return;
        }
        totalRetainedBytes.addAndGet(-entry.retainedBytes);
        entry.retainedBytes = 0;
        if (entry.spool != null) {
            entry.spool.close();
        }
        jobs.remove(queryId);
    }

    private void expireEntry(String queryId, AsyncQueryJobEntry entry) {
        if (entry == null || entry.job.status() == AsyncQueryJob.Status.EXPIRED) {
            return;
        }
        totalRetainedBytes.addAndGet(-entry.retainedBytes);
        entry.retainedBytes = 0;
        if (entry.spool != null) {
            entry.spool.close();
        }
        entry.job.expire();
        if (entry.completedAtEpochMs < 0) {
            entry.completedAtEpochMs = System.currentTimeMillis();
        }
    }

    private static boolean isTerminal(AsyncQueryJob.Status status) {
        return status == AsyncQueryJob.Status.SUCCEEDED
                || status == AsyncQueryJob.Status.FAILED
                || status == AsyncQueryJob.Status.EXPIRED
                || status == AsyncQueryJob.Status.CANCELLED;
    }
}
