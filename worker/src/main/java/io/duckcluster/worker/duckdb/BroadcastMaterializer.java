package io.duckcluster.worker.duckdb;

import io.duckcluster.common.planner.BroadcastSqlNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Materializes multi-shard broadcast tables once per worker query instead of UNION ALL per fragment. */
public final class BroadcastMaterializer {
    private static final Logger LOG = LoggerFactory.getLogger(BroadcastMaterializer.class);

    private final DuckDBConnectionPool pool;
    private final Set<String> materializedTables = ConcurrentHashMap.newKeySet();

    public BroadcastMaterializer(DuckDBConnectionPool pool) {
        this.pool = pool;
    }

    public String materialize(String tableName, int shardCount) throws SQLException, InterruptedException {
        if (shardCount <= 1) {
            return BroadcastSqlNames.tempTable(tableName);
        }
        String tempTable = BroadcastSqlNames.tempTable(tableName);
        Connection connection = pool.checkout();
        if (connection == null) {
            throw new SQLException("Connection pool exhausted while materializing broadcast " + tableName);
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + quote(tempTable));
            statement.execute("CREATE TABLE " + quote(tempTable) + " AS (" + unionAllSql(tableName, shardCount) + ")");
            materializedTables.add(tempTable);
            LOG.info("Materialized broadcast {} ({} shards) as {}", tableName, shardCount, tempTable);
            return tempTable;
        } finally {
            pool.checkin(connection);
        }
    }

    public void clearAll() throws SQLException, InterruptedException {
        if (materializedTables.isEmpty()) {
            return;
        }
        Connection connection = pool.checkout();
        if (connection == null) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            for (String tableName : List.copyOf(materializedTables)) {
                statement.execute("DROP TABLE IF EXISTS " + quote(tableName));
            }
            materializedTables.clear();
        } finally {
            pool.checkin(connection);
        }
    }

    private static String unionAllSql(String tableName, int shardCount) {
        StringBuilder sql = new StringBuilder();
        for (int shardId = 0; shardId < shardCount; shardId++) {
            if (shardId > 0) {
                sql.append(" UNION ALL ");
            }
            sql.append("SELECT * FROM ")
                    .append(quote(tableName + "_shard" + shardId))
                    .append(".")
                    .append(quote(tableName));
        }
        return sql.toString();
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
