package io.duckcluster.coordinator.monitor;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.planner.CalciteQueryPlanner;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.common.routing.ConsistentHashRing;
import io.duckcluster.coordinator.catalog.ShardCatalog;
import io.duckcluster.coordinator.execution.AsyncQueryService;
import io.duckcluster.coordinator.execution.QueryAdmissionControl;
import io.duckcluster.coordinator.execution.QueryExecutionService;
import io.duckcluster.coordinator.merger.MergeExecutionService;
import io.duckcluster.coordinator.merger.MergeStrategyRegistry;
import io.duckcluster.coordinator.worker.WorkerChannelPool;
import io.duckcluster.coordinator.worker.WorkerNodeClient;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MonitorServiceProfileTest {

    @Test
    void returnsAsyncStatusProfile() throws Exception {
        ClusterConfig config = ClusterConfig.fromEnvironment();
        WorkerRegistry registry = new WorkerRegistry();
        ClusterCatalog catalog = ClusterCatalog.demo(1);
        ShardCatalog shardCatalog = new ShardCatalog(new ConsistentHashRing(10), 1, catalog);
        QueryActivityTracker tracker = new QueryActivityTracker();
        QueryAdmissionControl admission = new QueryAdmissionControl(4);
        MergeExecutionService mergeExecution = new MergeExecutionService(2);
        QueryExecutionService executionService = new QueryExecutionService(
                new CalciteQueryPlanner(),
                catalog,
                registry,
                new WorkerNodeClient(new WorkerChannelPool(), 5_000),
                new MergeStrategyRegistry(),
                shardCatalog,
                config,
                tracker,
                admission,
                mergeExecution);
        AsyncQueryService async = new AsyncQueryService(executionService, tracker, config, admission);
        MonitorService monitor = new MonitorService(
                config, registry, shardCatalog, catalog, tracker, executionService);

        String queryId = async.submit("SELECT FROM");
        for (int i = 0; i < 50; i++) {
            if (async.status(queryId).map(s -> s.status().name()).orElse("").equals("FAILED")) {
                break;
            }
            Thread.sleep(20);
        }

        Map<String, Object> profile = monitor.queryProfile(queryId, async).orElseThrow();
        assertEquals(queryId, profile.get("queryId"));
        assertEquals("FAILED", profile.get("state"));
        mergeExecution.close();
    }
}
