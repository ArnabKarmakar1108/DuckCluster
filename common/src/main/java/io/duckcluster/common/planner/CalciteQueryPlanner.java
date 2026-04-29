package io.duckcluster.common.planner;

import io.duckcluster.common.model.BroadcastTable;
import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TableShardConfig;
import io.duckcluster.common.model.TopKSpec;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class CalciteQueryPlanner implements QueryPlanner {
    private static final SqlParser.Config PARSER_CONFIG = SqlParser.config()
            .withLex(Lex.JAVA)
            .withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    @Override
    public PlannedQuery plan(String sql, ClusterCatalog catalog) {
        SqlNode parsed = parse(sql);
        SqlSelect select = asSelect(parsed);

        TableClassification classification = classifyTables(select, catalog);
        int shardCount = classification.shardCount();

        MergeStrategyType mergeStrategy = detectMergeStrategy(parsed);
        TopKSpec topK = TopKExtractor.extract(parsed);
        QueryAnalysis analysis = QueryAnalysisExtractor.withMergeColumnNames(
                QueryAnalysisExtractor.extract(select, mergeStrategy));

        String drivingTable = classification.drivingTable();
        Map<String, Integer> broadcastShardCounts = new HashMap<>();
        for (BroadcastTable bt : classification.broadcastTables()) {
            broadcastShardCounts.put(bt.tableName().toLowerCase(), bt.shardCount());
        }

        List<FragmentSpec> fragments = new ArrayList<>(shardCount);
        for (int shardId = 0; shardId < shardCount; shardId++) {
            String fragmentSql = FragmentSqlGenerator.generate(
                    select, shardId, analysis, drivingTable, broadcastShardCounts, topK);
            fragments.add(new FragmentSpec(shardId, shardId, fragmentSql, mergeStrategy));
        }

        return new PlannedQuery(sql, List.of(drivingTable), classification.broadcastTables(),
                fragments, mergeStrategy, analysis, List.of(), topK);
    }

    public SqlNode parse(String sql) {
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            return parser.parseStmt();
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("Invalid SQL: " + e.getMessage(), e);
        }
    }

    public MergeStrategyType detectMergeStrategy(SqlNode parsed) {
        SqlSelect select = asSelect(parsed);
        if (select.getGroup() != null && !select.getGroup().isEmpty()) {
            return MergeStrategyType.GROUP_BY_MERGE;
        }
        if (containsAggregate(select.getSelectList())) {
            return MergeStrategyType.PARTIAL_AGG;
        }
        if (hasTopK(parsed, select)) {
            return MergeStrategyType.TOP_K;
        }
        return MergeStrategyType.CONCATENATE;
    }

    static TableClassification classifyTables(SqlSelect select, ClusterCatalog catalog) {
        List<String> allTables = collectTableNames(select.getFrom());
        if (allTables.isEmpty()) {
            throw new IllegalArgumentException("No tables found in FROM clause");
        }

        for (String table : allTables) {
            if (!catalog.hasTable(table)) {
                throw new IllegalArgumentException("Unknown table: " + table
                        + ". Known tables: " + catalog.getShardedTableNames());
            }
        }

        String drivingTable = allTables.get(0);
        int maxShards = catalog.table(drivingTable).shardCount();
        for (String t : allTables) {
            int count = catalog.table(t).shardCount();
            if (count > maxShards) {
                drivingTable = t;
                maxShards = count;
            }
        }

        List<BroadcastTable> broadcast = new ArrayList<>();
        for (String t : allTables) {
            if (!t.equalsIgnoreCase(drivingTable)) {
                broadcast.add(new BroadcastTable(t, catalog.table(t).shardCount()));
            }
        }

        return new TableClassification(drivingTable, broadcast, maxShards);
    }

    static List<String> collectTableNames(SqlNode from) {
        List<String> tables = new ArrayList<>();
        collectTableNamesRecursive(from, tables);
        return tables;
    }

    private static void collectTableNamesRecursive(SqlNode node, List<String> tables) {
        if (node instanceof SqlIdentifier identifier) {
            if (identifier.names.size() == 1) {
                tables.add(identifier.getSimple());
            }
        } else if (node instanceof SqlJoin join) {
            collectTableNamesRecursive(join.getLeft(), tables);
            collectTableNamesRecursive(join.getRight(), tables);
        } else if (node instanceof SqlBasicCall call && call.getKind() == SqlKind.AS) {
            collectTableNamesRecursive(call.operand(0), tables);
        }
    }

    private static SqlSelect asSelect(SqlNode parsed) {
        if (parsed instanceof SqlOrderBy orderBy) {
            if (orderBy.query instanceof SqlSelect select) {
                return select;
            }
            throw new IllegalArgumentException("Only SELECT queries are supported");
        }
        if (parsed instanceof SqlSelect select) {
            return select;
        }
        throw new IllegalArgumentException("Only SELECT queries are supported");
    }

    private static boolean hasTopK(SqlNode parsed, SqlSelect select) {
        if (parsed instanceof SqlOrderBy orderBy) {
            return (orderBy.orderList != null && !orderBy.orderList.isEmpty()) || orderBy.fetch != null;
        }
        return (select.getOrderList() != null && !select.getOrderList().isEmpty()) || select.getFetch() != null;
    }

    private static boolean containsAggregate(SqlNode node) {
        if (node == null) {
            return false;
        }
        AggregateDetector detector = new AggregateDetector();
        node.accept(detector);
        return detector.foundAggregate();
    }

    private static final class AggregateDetector extends SqlShuttle {
        private boolean aggregate;

        @Override
        public SqlNode visit(SqlCall call) {
            if (AggregateSqlSupport.isAggregateCall(call)) {
                aggregate = true;
            }
            return super.visit(call);
        }

        boolean foundAggregate() {
            return aggregate;
        }
    }

    record TableClassification(String drivingTable, List<BroadcastTable> broadcastTables, int shardCount) {}
}
