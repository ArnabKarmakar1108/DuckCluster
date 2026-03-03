package io.duckcluster.coordinator.http;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.execution.QueryExecutionService;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CoordinatorHttpServer {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorHttpServer.class);

    private final ClusterConfig config;
    private final WorkerRegistry registry;
    private final QueryExecutionService queryExecutionService;
    private Javalin app;

    public CoordinatorHttpServer(
            ClusterConfig config, WorkerRegistry registry, QueryExecutionService queryExecutionService) {
        this.config = config;
        this.registry = registry;
        this.queryExecutionService = queryExecutionService;
    }

    public void start() {
        app = Javalin.create(cfg -> cfg.showJavalinBanner = false)
                .post("/v1/query", ctx -> {
                    QueryRequest request = ctx.bodyAsClass(QueryRequest.class);
                    if (request.sql == null || request.sql.isBlank()) {
                        ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", "sql is required"));
                        return;
                    }
                    QueryResult result = queryExecutionService.execute(request.sql);
                    ctx.json(toResponse(result));
                })
                .get("/v1/cluster/health", ctx -> {
                    Map<String, Object> body = new LinkedHashMap<>();
                    body.put("status", registry.isHealthy() ? "UP" : "DEGRADED");
                    body.put("workerCount", registry.workerCount());
                    ctx.json(body);
                })
                .get("/v1/cluster/workers", ctx -> {
                    List<Map<String, Object>> workers = registry.listWorkers().stream()
                            .map(worker -> {
                                Map<String, Object> entry = new LinkedHashMap<>();
                                entry.put("workerId", worker.workerId());
                                entry.put("host", worker.host());
                                entry.put("port", worker.port());
                                entry.put("numThreads", worker.numThreads());
                                entry.put("status", worker.status().name());
                                entry.put("registeredAt", worker.registeredAt().toString());
                                entry.put("lastHeartbeatAt", worker.lastHeartbeatAt().toString());
                                entry.put("load", worker.load());
                                return entry;
                            })
                            .toList();
                    ctx.json(Map.of("workers", workers));
                })
                .get("/", ctx -> ctx.result("DuckCluster coordinator"))
                .exception(IllegalArgumentException.class, (exception, ctx) -> {
                    ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", exception.getMessage()));
                })
                .exception(IllegalStateException.class, (exception, ctx) -> {
                    ctx.status(HttpStatus.SERVICE_UNAVAILABLE).json(Map.of("error", exception.getMessage()));
                })
                .exception(Exception.class, (exception, ctx) -> {
                    LOG.error("HTTP error", exception);
                    ctx.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .json(Map.of("error", exception.getMessage()));
                })
                .start(config.coordinatorHost(), config.coordinatorHttpPort());

        LOG.info("Coordinator HTTP listening on {}:{}", config.coordinatorHost(), config.coordinatorHttpPort());
    }

    public void stop() {
        if (app != null) {
            app.stop();
        }
    }

    private static Map<String, Object> toResponse(QueryResult result) {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("mergeStrategy", result.stats().mergeStrategy().name());
        stats.put("workersUsed", result.stats().workersUsed());
        stats.put("fragmentsExecuted", result.stats().fragmentsExecuted());
        stats.put("durationMs", result.stats().durationMs());
        stats.put("workerDurationsMs", result.stats().workerDurationsMs());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("queryId", result.queryId());
        response.put("columns", result.columns());
        response.put("rows", result.rows());
        response.put("stats", stats);
        return response;
    }

    private static final class QueryRequest {
        public String sql;
    }
}
