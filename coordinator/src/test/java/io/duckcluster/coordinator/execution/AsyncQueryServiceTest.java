package io.duckcluster.coordinator.execution;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.planner.CalciteQueryPlanner;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.common.routing.ConsistentHashRing;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.merger.MergeExecutionService;
import io.duckcluster.coordinator.merger.MergeStrategyRegistry;
import io.duckcluster.coordinator.monitor.QueryActivityTracker;
import io.duckcluster.coordinator.worker.WorkerChannelPool;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncQueryServiceTest {

    @Test
    void evictsOldestJobWhenOverMaxStoredJobs() throws Exception {
        ClusterConfig config = testConfig(50L, 1, 1024 * 1024);
        AsyncQueryService async = newAsyncService(config);
        String first = async.submit("SELECT FROM");
        waitForStatus(async, first, AsyncQueryJob.Status.FAILED);
        String second = async.submit("SELECT FROM");
        waitForStatus(async, second, AsyncQueryJob.Status.FAILED);
        assertTrue(async.status(first).isEmpty());
        assertEquals(AsyncQueryJob.Status.FAILED, async.status(second).orElseThrow().status());
    }

    @Test
    void expiresTerminalJobsAfterTtl() throws Exception {
        ClusterConfig config = testConfig(50L, 10, 1024 * 1024);

        AsyncQueryService async = newAsyncService(config);
        String queryId = async.submit("SELECT FROM");
        waitForStatus(async, queryId, AsyncQueryJob.Status.FAILED);
        // Past TTL (50ms) but before tombstone eviction (2× TTL).
        Thread.sleep(65);
        assertEquals(AsyncQueryJob.Status.EXPIRED, async.status(queryId).orElseThrow().status());
    }

    private static AsyncQueryService newAsyncService(ClusterConfig config) {
        QueryAdmissionControl admission = new QueryAdmissionControl(4);
        QueryActivityTracker tracker = new QueryActivityTracker();
        ClusterCatalog catalog = ClusterCatalog.demo(1);
        WorkerRegistry registry = new WorkerRegistry();
        ShardCatalog shardCatalog = new ShardCatalog(new ConsistentHashRing(10), 1, catalog);
        WorkerNodeClient workerClient = new WorkerNodeClient(new WorkerChannelPool(), 5_000);
        MergeExecutionService mergeExecution = new MergeExecutionService(2);
        QueryExecutionService executionService = new QueryExecutionService(
                new CalciteQueryPlanner(),
                catalog,
                registry,
                workerClient,
                new MergeStrategyRegistry(),
                shardCatalog,
                config,
                tracker,
                admission,
                mergeExecution);
        return new AsyncQueryService(executionService, tracker, config, admission);
    }

    private static ClusterConfig testConfig(long asyncTtlMs, int maxStoredJobs, long maxStoredBytes) {
        return new ClusterConfig(
                "127.0.0.1",
                9090,
                8080,
                "127.0.0.1",
                Duration.ofSeconds(5),
                6,
                1,
                "./data",
                2,
                200,
                1,
                100,
                2000,
                32,
                60_000,
                false,
                asyncTtlMs,
                maxStoredJobs,
                maxStoredBytes,
                Duration.ofMinutes(10).toMillis(),
                Duration.ofSeconds(30).toMillis(),
                4,
                2,
                2,
                4,
                60_000,
                2,
                512,
                1024,
                Duration.ofMinutes(5).toMillis(),
                2,
                30_000,
                1024,
                64 * 1024,
                8 * 1024 * 1024,
                false);
    }

    private static void waitForStatus(AsyncQueryService async, String queryId, AsyncQueryJob.Status expected)
            throws InterruptedException {
        for (int i = 0; i < 50; i++) {
            AsyncQueryService.StatusView status = async.status(queryId).orElseThrow();
            if (status.status() == expected) {
                return;
            }
            Thread.sleep(20);
        }
        assertEquals(expected, async.status(queryId).orElseThrow().status());
    }
}
