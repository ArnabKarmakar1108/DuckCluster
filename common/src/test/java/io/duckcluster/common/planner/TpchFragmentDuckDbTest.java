package io.duckcluster.common.planner;

import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.PlannedQuery;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

class TpchFragmentDuckDbTest {
    private static final Path TPCH_WORKERS_ROOT = Path.of("..", "benchmark", "data", "sf0.01", "workers")
            .normalize();

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final io.duckcluster.common.model.ClusterCatalog tpch = TpchCatalog.create();

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "q01", "q02", "q03", "q04", "q05", "q06", "q07", "q08", "q09", "q10", "q11",
            "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22"
    })
    void fragmentSqlExecutesOnTpchShards(String queryId) throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(TPCH_WORKERS_ROOT), "TPC-H worker shards not generated");

        PlannedQuery planned = planner.plan(load(queryId), tpch);
        FragmentSpec fragment = planned.fragments().get(0);

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement()) {
            attachAllWorkerShards(statement);
            statement.execute("SELECT * FROM (" + fragment.sql() + ") AS fragment_probe LIMIT 1");
        }
    }

    private static void attachAllWorkerShards(Statement statement) throws Exception {
        Set<String> attached = new HashSet<>();
        try (Stream<Path> workerDirs = Files.list(TPCH_WORKERS_ROOT)) {
            for (Path workerDir : workerDirs.filter(Files::isDirectory).toList()) {
                try (Stream<Path> files = Files.list(workerDir)) {
                    for (Path shardFile : files.filter(path -> path.toString().endsWith(".duckdb")).toList()) {
                        String fileName = shardFile.getFileName().toString();
                        String catalogName = fileName.substring(0, fileName.length() - ".duckdb".length());
                        if (!attached.add(catalogName)) {
                            continue;
                        }
                        statement.execute("ATTACH '" + shardFile.toAbsolutePath()
                                + "' AS \"" + catalogName + "\" (READ_ONLY)");
                    }
                }
            }
        }
    }

    private String load(String queryId) throws Exception {
        Path path = Path.of("..", "tests", "integration", "queries", "tpch", queryId + ".sql");
        String sql = Files.readString(path);
        return sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql;
    }
}
