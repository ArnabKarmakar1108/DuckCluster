package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import io.duckcluster.common.model.MergeStrategy;
import io.duckcluster.common.model.PlannedQuery;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public final class CalciteQueryPlanner implements QueryPlanner {
    private static final SqlParser.Config PARSER_CONFIG = SqlParser.config()
            .withLex(Lex.JAVA)
            .withConformance(SqlConformanceEnum.DEFAULT);

    @Override
    public PlannedQuery plan(String sql, ClusterCatalog catalog) {
        SqlNode parsed = parse(sql);
        MergeStrategy mergeStrategy = detectMergeStrategy(parsed);
        return new PlannedQuery(sql, java.util.List.of(), mergeStrategy, java.util.List.of());
    }

    public SqlNode parse(String sql) {
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            return parser.parseStmt();
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("Invalid SQL: " + e.getMessage(), e);
        }
    }

    public MergeStrategy detectMergeStrategy(SqlNode parsed) {
        if (parsed instanceof SqlSelect select) {
            if (select.getGroup() != null && !select.getGroup().isEmpty()) {
                return MergeStrategy.GROUP_BY_MERGE;
            }
            if (containsAggregate(select.getSelectList())) {
                return MergeStrategy.PARTIAL_AGG;
            }
            if ((select.getOrderList() != null && !select.getOrderList().isEmpty()) || select.getFetch() != null) {
                return MergeStrategy.TOP_K;
            }
            return MergeStrategy.CONCATENATE;
        }

        MergeStrategyDetector detector = new MergeStrategyDetector();
        parsed.accept(detector);
        return detector.strategy();
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

        MergeStrategy strategy() {
            if (aggregate) {
                return MergeStrategy.PARTIAL_AGG;
            }
            if (topK) {
                return MergeStrategy.TOP_K;
            }
            return MergeStrategy.CONCATENATE;
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
