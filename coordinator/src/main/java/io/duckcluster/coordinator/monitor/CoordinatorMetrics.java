package io.duckcluster.coordinator.monitor;

import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.execution.QueryAdmissionControl;
import io.duckcluster.common.registry.WorkerRegistry;

import java.time.Duration;
import java.time.Instant;

/** Minimal Prometheus text exposition without external dependencies. */
public final class CoordinatorMetrics {
    private final WorkerRegistry registry;
    private final ShardCatalog shardCatalog;
    private final QueryActivityTracker activityTracker;
    private final QueryAdmissionControl admission;
    private final int maxConcurrentQueries;
    private final Duration heartbeatTimeout;

    public CoordinatorMetrics(
            WorkerRegistry registry,
            ShardCatalog shardCatalog,
            QueryActivityTracker activityTracker,
            QueryAdmissionControl admission,
            int maxConcurrentQueries,
            Duration heartbeatTimeout) {
        this.registry = registry;
        this.shardCatalog = shardCatalog;
        this.activityTracker = activityTracker;
        this.admission = admission;
        this.maxConcurrentQueries = maxConcurrentQueries;
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public String prometheusText() {
        StringBuilder out = new StringBuilder();
        int workers = registry.workerCount();
        int healthy = countHealthyWorkers();
        int activeQueries = activityTracker.activeQueries().size();
        int underReplicated = shardCatalog.getUnderReplicatedShards().size();
        int admissionAvailable = admission.availablePermits();

        appendMetric(out, "duckcluster_workers_total", workers);
        appendMetric(out, "duckcluster_workers_healthy", healthy);
        appendMetric(out, "duckcluster_queries_active", activeQueries);
        appendMetric(out, "duckcluster_admission_available", admissionAvailable);
        appendMetric(out, "duckcluster_admission_max", maxConcurrentQueries);
        appendMetric(out, "duckcluster_shards_under_replicated", underReplicated);
        return out.toString();
    }

    private int countHealthyWorkers() {
        Instant now = Instant.now();
        int healthy = 0;
        for (WorkerRegistry.WorkerRecord worker : registry.listWorkers()) {
            long heartbeatAgeMs = Duration.between(worker.lastHeartbeatAt(), now).toMillis();
            boolean stale = heartbeatAgeMs > heartbeatTimeout.toMillis();
            if (worker.status() == WorkerRegistry.WorkerStatus.HEALTHY && !stale) {
                healthy++;
            }
        }
        return healthy;
    }

    private static void appendMetric(StringBuilder out, String name, long value) {
        out.append("# TYPE ").append(name).append(" gauge\n");
        out.append(name).append(' ').append(value).append('\n');
    }
}
