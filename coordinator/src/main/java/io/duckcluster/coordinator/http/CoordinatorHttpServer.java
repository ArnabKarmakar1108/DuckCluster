package io.duckcluster.coordinator.http;

import io.duckcluster.common.config.ClusterConfig;
import io.duckcluster.common.model.QueryResult;
import io.duckcluster.common.registry.WorkerRegistry;
import io.duckcluster.coordinator.execution.AsyncQueryExpiredException;
import io.duckcluster.coordinator.execution.AsyncQueryJob;
import io.duckcluster.coordinator.execution.AsyncQueryService;
import io.duckcluster.coordinator.execution.QueryExecutionService;
import io.duckcluster.coordinator.monitor.MonitorService;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import io.javalin.http.staticfiles.Location;
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
    private final AsyncQueryService asyncQueryService;
    private final MonitorService monitorService;
    private Javalin app;

    public CoordinatorHttpServer(
            ClusterConfig config,
            WorkerRegistry registry,
            QueryExecutionService queryExecutionService,
            AsyncQueryService asyncQueryService,
            MonitorService monitorService) {
        this.config = config;
        this.registry = registry;
        this.queryExecutionService = queryExecutionService;
        this.asyncQueryService = asyncQueryService;
        this.monitorService = monitorService;
    }

    public void start() {
        app = Javalin.create(cfg -> {
                    cfg.showJavalinBanner = false;
                    cfg.staticFiles.add(staticFiles -> {
                        staticFiles.hostedPath = "/dashboard";
                        staticFiles.directory = "dashboard";
                        staticFiles.location = Location.CLASSPATH;
                    });
                })
                .post("/v1/query", ctx -> {
                    QueryRequest request = ctx.bodyAsClass(QueryRequest.class);
                    if (request.sql == null || request.sql.isBlank()) {
                        ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", "sql is required"));
                        return;
                    }
                    QueryResult result = queryExecutionService.execute(request.sql);
                    ctx.json(QueryResponseMapper.toFullResponse(result));
                })
                .post("/v1/query/async", ctx -> {
                    QueryRequest request = ctx.bodyAsClass(QueryRequest.class);
                    if (request.sql == null || request.sql.isBlank()) {
                        ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", "sql is required"));
                        return;
                    }
                    String queryId = asyncQueryService.submit(request.sql);
                    Map<String, Object> body = new LinkedHashMap<>();
                    body.put("queryId", queryId);
                    body.put("status", AsyncQueryJob.Status.QUEUED.name());
                    body.put("statusUrl", "/v1/query/" + queryId);
                    body.put("resultsUrl", "/v1/query/" + queryId + "/results");
                    ctx.status(HttpStatus.ACCEPTED).json(body);
                })
                .get("/v1/query/{queryId}", ctx -> {
                    String queryId = ctx.pathParam("queryId");
                    asyncQueryService.status(queryId).ifPresentOrElse(
                            status -> {
                                if (status.status() == AsyncQueryJob.Status.EXPIRED) {
                                    ctx.status(HttpStatus.GONE)
                                            .json(Map.of(
                                                    "queryId", queryId,
                                                    "status", AsyncQueryJob.Status.EXPIRED.name(),
                                                    "error", "query result expired"));
                                    return;
                                }
                                ctx.json(toStatusResponse(status));
                            },
                            () -> ctx.status(HttpStatus.NOT_FOUND).json(Map.of("error", "unknown query id")));
                })
                .get("/v1/query/{queryId}/results", ctx -> {
                    String queryId = ctx.pathParam("queryId");
                    int offset = parseIntQuery(ctx, "offset", 0);
                    int limit = parseIntQuery(ctx, "limit", AsyncQueryService.DEFAULT_RESULT_LIMIT);
                    try {
                        AsyncQueryService.ResultChunk chunk = asyncQueryService.resultChunk(queryId, offset, limit);
                        ctx.json(QueryResponseMapper.toResultChunk(
                                chunk.result(),
                                chunk.offset(),
                                chunk.limit(),
                                chunk.rows(),
                                chunk.nextOffset()));
                    } catch (AsyncQueryExpiredException exception) {
                        ctx.status(HttpStatus.GONE)
                                .json(Map.of(
                                        "queryId", queryId,
                                        "status", AsyncQueryJob.Status.EXPIRED.name(),
                                        "error", "query result expired"));
                    } catch (IllegalArgumentException exception) {
                        ctx.status(HttpStatus.NOT_FOUND).json(Map.of("error", exception.getMessage()));
                    } catch (IllegalStateException exception) {
                        ctx.status(HttpStatus.CONFLICT).json(Map.of("error", exception.getMessage()));
                    }
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
                .get("/v1/monitor/summary", ctx -> ctx.json(monitorService.summary()))
                .get("/v1/monitor/shards", ctx -> ctx.json(monitorService.shards()))
                .get("/", ctx -> ctx.redirect("/dashboard/index.html"))
                .get("/dashboard", ctx -> ctx.redirect("/dashboard/index.html"))
                .get("/dashboard/", ctx -> ctx.redirect("/dashboard/index.html"))
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
                .start(config.coordinatorHttpBindHost(), config.coordinatorHttpPort());

        LOG.info(
                "Coordinator HTTP listening on {}:{} (workers connect via {}:{})",
                config.coordinatorHttpBindHost(),
                config.coordinatorHttpPort(),
                config.coordinatorHost(),
                config.coordinatorHttpPort());
    }

    public void stop() {
        if (app != null) {
            app.stop();
        }
    }

    private static Map<String, Object> toStatusResponse(AsyncQueryService.StatusView status) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("queryId", status.queryId());
        body.put("status", status.status().name());
        if (status.phase() != null) {
            body.put("phase", status.phase().name());
        }
        if (status.status() == AsyncQueryJob.Status.RUNNING || status.status() == AsyncQueryJob.Status.QUEUED) {
            body.put("fragmentsDone", status.fragmentsDone());
            body.put("fragmentsTotal", status.fragmentsTotal());
            body.put("elapsedMs", status.elapsedMs());
        }
        if (status.error() != null) {
            body.put("error", status.error());
        }
        if (status.stats() != null) {
            body.put("stats", QueryResponseMapper.toStatsMap(status.stats()));
        }
        return body;
    }

    private static int parseIntQuery(io.javalin.http.Context ctx, String name, int defaultValue) {
        String raw = ctx.queryParam(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("invalid " + name + ": " + raw);
        }
    }

    private static final class QueryRequest {
        public String sql;
    }
}
