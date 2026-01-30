package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.FragmentSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.PlannedQuery;
import io.duckcluster.common.model.TableShardConfig;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
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
            .withConformance(SqlConformanceEnum.DEFAULT);

    @Override
    public PlannedQuery plan(String sql, ClusterCatalog catalog) {
        SqlNode parsed = parse(sql);
        if (!(parsed instanceof SqlSelect)) {
            throw new IllegalArgumentException("Only SELECT queries are supported");
        }

        String tableName = extractTableName((SqlSelect) parsed);
        if (!catalog.hasTable(tableName)) {
            throw new IllegalArgumentException("Table is not registered in cluster catalog: " + tableName);
        }

        TableShardConfig tableConfig = catalog.table(tableName);
        MergeStrategyType mergeStrategy = detectMergeStrategy(parsed);
        List<FragmentSpec> fragments = buildFragments(sql, tableConfig, mergeStrategy);
        return new PlannedQuery(sql, tableName, fragments, mergeStrategy, List.of());
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
        if (parsed instanceof SqlSelect select) {
            if (select.getGroup() != null && !select.getGroup().isEmpty()) {
                return MergeStrategyType.GROUP_BY_MERGE;
            }
            if (containsAggregate(select.getSelectList())) {
                return MergeStrategyType.PARTIAL_AGG;
            }
            if ((select.getOrderList() != null && !select.getOrderList().isEmpty())
                    || select.getFetch() != null) {
                return MergeStrategyType.TOP_K;
            }
            return MergeStrategyType.CONCATENATE;
        }

        MergeStrategyDetector detector = new MergeStrategyDetector();
        parsed.accept(detector);
        return detector.strategy();
    }

    private static List<FragmentSpec> buildFragments(
            String sql, TableShardConfig tableConfig, MergeStrategyType mergeStrategy) {
        List<FragmentSpec> fragments = new ArrayList<>(tableConfig.shardCount());
        for (int shardId = 0; shardId < tableConfig.shardCount(); shardId++) {
            String fragmentSql = ShardPredicateInjector.inject(
                    sql, tableConfig.shardKey(), tableConfig.shardCount(), shardId);
            fragments.add(new FragmentSpec(shardId, shardId, fragmentSql, mergeStrategy));
        }
        return fragments;
    }

    private static String extractTableName(SqlSelect select) {
        SqlNode from = select.getFrom();
        if (from instanceof SqlIdentifier identifier) {
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

    private static final class MergeStrategyDetector extends SqlShuttle {
        private boolean aggregate;
        private boolean topK;

        @Override
        public SqlNode visit(SqlCall call) {
            if (call.getKind().belongsTo(SqlKind.AGGREGATE)) {
                aggregate = true;
            }
            if (call.getKind() == SqlKind.ORDER_BY) {
                topK = true;
            }
            return super.visit(call);
        }

        MergeStrategyType strategy() {
            if (aggregate) {
                return MergeStrategyType.PARTIAL_AGG;
            }
            if (topK) {
                return MergeStrategyType.TOP_K;
            }
            return MergeStrategyType.CONCATENATE;
        }
    }

    private static final class AggregateDetector extends SqlShuttle {
        private boolean aggregate;

        @Override
        public SqlNode visit(SqlCall call) {
            if (call.getKind().belongsTo(SqlKind.AGGREGATE)) {
                aggregate = true;
            }
            return super.visit(call);
        }

        boolean foundAggregate() {
            return aggregate;
        }
    }
}
