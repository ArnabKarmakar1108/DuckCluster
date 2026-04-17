package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TableShardConfig;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.ArrayList;
import java.util.List;

public final class CalciteQueryPlanner implements QueryPlanner {
    private static final SqlParser.Config PARSER_CONFIG = SqlParser.config()
            .withLex(Lex.JAVA)
            .withConformance(SqlConformanceEnum.PRAGMATIC_2003);

    @Override
    public PlannedQuery plan(String sql, ClusterCatalog catalog) {
        SqlNode parsed = parse(sql);
        SqlSelect select = asSelect(parsed);
        String tableName = extractTableName(select);
        if (!catalog.hasTable(tableName)) {
            throw new IllegalArgumentException("Table is not registered in cluster catalog: " + tableName);
        }

        TableShardConfig tableConfig = catalog.table(tableName);
        MergeStrategyType mergeStrategy = detectMergeStrategy(select);
        QueryAnalysis analysis = QueryAnalysisExtractor.withMergeColumnNames(
                QueryAnalysisExtractor.extract(select, mergeStrategy));
        List<FragmentSpec> fragments = buildFragments(select, tableConfig, mergeStrategy, analysis);
        return new PlannedQuery(sql, tableName, fragments, mergeStrategy, analysis, List.of());
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

    private static List<FragmentSpec> buildFragments(
            SqlSelect select,
            TableShardConfig tableConfig,
            MergeStrategyType mergeStrategy,
            QueryAnalysis analysis) {
        List<FragmentSpec> fragments = new ArrayList<>(tableConfig.shardCount());
        for (int shardId = 0; shardId < tableConfig.shardCount(); shardId++) {
            String fragmentSql = FragmentSqlGenerator.generate(select, tableConfig, shardId, analysis);
            fragments.add(new FragmentSpec(shardId, shardId, fragmentSql, mergeStrategy));
        }
        return fragments;
    }

    private static String extractTableName(SqlSelect select) {
        SqlNode from = select.getFrom();
        if (from instanceof org.apache.calcite.sql.SqlIdentifier identifier) {
            return identifier.getSimple();
        }
        throw new IllegalArgumentException("Only single-table SELECT queries are supported");
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
}
