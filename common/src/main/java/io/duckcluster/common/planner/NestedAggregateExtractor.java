package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.ComputedOutputSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.List;

/** Extracts nested aggregates from arithmetic SELECT items and builds merge expressions. */
final class NestedAggregateExtractor {
    private static final SqlDialect DIALECT = DuckDBSqlDialect.DEFAULT;

    private NestedAggregateExtractor() {}

    record Analysis(
            String outputName,
            List<AggregateSpec> aggregates,
            ComputedOutputSpec computedOutput,
            int nextAggregateIndex) {}

    static boolean containsNestedAggregate(SqlNode item) {
        if (AggregateSqlSupport.isAggregateExpression(item)) {
            return false;
        }
        AggregateCollector collector = new AggregateCollector();
        AggregateSqlSupport.unwrapAlias(item).accept(collector);
        return !collector.aggregateCalls().isEmpty();
    }

    static List<SqlCall> collectAggregateCalls(SqlNode expression) {
        AggregateCollector collector = new AggregateCollector();
        expression.accept(collector);
        return collector.aggregateCalls();
    }

    static Analysis analyze(SqlNode item, int aggregateIndex) {
        SqlNode expression = AggregateSqlSupport.unwrapAlias(item);
        String outputName = outputName(item);
        List<SqlCall> aggregateCalls = collectAggregateCalls(expression);

        List<AggregateSpec> aggregates = new ArrayList<>(aggregateCalls.size());
        List<String> mergeColumnNames = new ArrayList<>(aggregateCalls.size());
        int index = aggregateIndex;
        for (SqlCall call : aggregateCalls) {
            AggregateOperand operand = extractOperand(call);
            AggregateFunction function = AggregateSqlSupport.toAggregateFunction(call);
            String mergeColumn = QueryAnalysisExtractor.mergeColumnName(index);
            aggregates.add(new AggregateSpec(
                    mergeColumn,
                    mergeColumn,
                    function,
                    operand.inputColumn(),
                    operand.inputExpression()));
            mergeColumnNames.add(mergeColumn);
            index++;
        }

        SqlNode mergeExpression = expression.accept(new MergeExpressionShuttle(mergeColumnNames, aggregateCalls));
        String mergeSql = toSql(mergeExpression);
        return new Analysis(
                outputName,
                aggregates,
                new ComputedOutputSpec(outputName, mergeSql),
                index);
    }

    private static String outputName(SqlNode item) {
        if (item instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.AS) {
            SqlNode aliasNode = call.operand(1);
            if (aliasNode instanceof SqlIdentifier alias) {
                return alias.getSimple();
            }
        }
        throw new IllegalArgumentException("Computed SELECT item requires an alias: " + item);
    }

    private static AggregateOperand extractOperand(SqlCall call) {
        if (call.getOperandList().isEmpty()) {
            return new AggregateOperand(null, null);
        }
        SqlNode operand = call.operand(0);
        if (operand instanceof SqlIdentifier identifier && identifier.isStar()) {
            return new AggregateOperand(null, null);
        }
        if (operand instanceof SqlIdentifier identifier) {
            return new AggregateOperand(columnName(identifier), null);
        }
        return new AggregateOperand(null, operand);
    }

    private static String columnName(SqlIdentifier identifier) {
        List<String> names = identifier.names;
        return names.get(names.size() - 1);
    }

    private static String toSql(SqlNode node) {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        node.unparse(writer, 0, 0);
        return writer.toSqlString().getSql();
    }

    private static final class AggregateCollector extends SqlShuttle {
        private final List<SqlCall> aggregateCalls = new ArrayList<>();

        @Override
        public SqlNode visit(SqlCall call) {
            if (AggregateSqlSupport.isAggregateCall(call)) {
                aggregateCalls.add(call);
                return call;
            }
            return super.visit(call);
        }

        List<SqlCall> aggregateCalls() {
            return aggregateCalls;
        }
    }

    private static final class MergeExpressionShuttle extends SqlShuttle {
        private final List<String> mergeColumnNames;
        private final List<SqlCall> aggregateCalls;
        private int aggregateIndex;

        private MergeExpressionShuttle(List<String> mergeColumnNames, List<SqlCall> aggregateCalls) {
            this.mergeColumnNames = mergeColumnNames;
            this.aggregateCalls = aggregateCalls;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (AggregateSqlSupport.isAggregateCall(call)) {
                SqlCall expected = aggregateCalls.get(aggregateIndex);
                if (call != expected && !call.equalsDeep(expected, org.apache.calcite.util.Litmus.IGNORE)) {
                    throw new IllegalStateException("Unexpected aggregate ordering in merge expression rewrite");
                }
                AggregateFunction function = AggregateSqlSupport.toAggregateFunction(call);
                String mergeColumn = mergeColumnNames.get(aggregateIndex++);
                SqlIdentifier column = new SqlIdentifier(mergeColumn, SqlParserPos.ZERO);
                SqlNode merged = switch (function) {
                    case COUNT, SUM -> SqlStdOperatorTable.SUM.createCall(SqlParserPos.ZERO, column);
                    case MIN -> SqlStdOperatorTable.MIN.createCall(SqlParserPos.ZERO, column);
                    case MAX -> SqlStdOperatorTable.MAX.createCall(SqlParserPos.ZERO, column);
                    case AVG -> throw new IllegalStateException("AVG in computed expression not supported yet");
                };
                return merged;
            }
            return super.visit(call);
        }
    }

    private record AggregateOperand(String inputColumn, SqlNode inputExpression) {}
}
