package io.duckcluster.common.duckdb;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class DuckDbResourceLimits {
    private DuckDbResourceLimits() {}

    public static void apply(Connection connection, long memoryLimitMb, int threads, Path tempDirectory)
            throws SQLException {
        try (Statement statement = connection.createStatement()) {
            if (memoryLimitMb > 0) {
                statement.execute("SET memory_limit='" + memoryLimitMb + "MB'");
            }
            if (threads > 0) {
                statement.execute("SET threads=" + threads);
            }
            if (tempDirectory != null) {
                statement.execute("SET temp_directory='" + tempDirectory.toAbsolutePath() + "'");
            }
        }
    }
}
