package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryAnalysis;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;

public final class QueryAnalysisExtractor {
    private QueryAnalysisExtractor() {}

    public static QueryAnalysis extract(SqlSelect select, MergeStrategyType mergeStrategy) {
        if (mergeStrategy == MergeStrategyType.CONCATENATE || mergeStrategy == MergeStrategyType.TOP_K) {
            return QueryAnalysis.empty();
        }

        List<String> groupByColumns = extractGroupByColumns(select);
        List<AggregateSpec> aggregates = new ArrayList<>();
        List<String> outputColumnNames = new ArrayList<>();

        int aggregateIndex = 0;
        for (SqlNode item : select.getSelectList()) {
            if (isAggregate(item)) {
                AggregateSpec aggregate = toAggregateSpec(item, aggregateIndex++);
                aggregates.add(aggregate);
                outputColumnNames.add(aggregate.outputName());
            } else if (mergeStrategy == MergeStrategyType.GROUP_BY_MERGE) {
                String columnName = columnName(item);
                outputColumnNames.add(columnName);
            }
        }

        return new QueryAnalysis(groupByColumns, aggregates, outputColumnNames);
    }

    public static QueryAnalysis withMergeColumnNames(QueryAnalysis analysis) {
        List<AggregateSpec> rewritten = new ArrayList<>(analysis.aggregates().size());
        for (int i = 0; i < analysis.aggregates().size(); i++) {
            AggregateSpec aggregate = analysis.aggregates().get(i);
            rewritten.add(new AggregateSpec(
                    aggregate.outputName(),
                    mergeColumnName(i),
                    aggregate.function(),
                    aggregate.inputColumn()));
        }
        return new QueryAnalysis(analysis.groupByColumns(), rewritten, analysis.outputColumnNames());
    }

    static String mergeColumnName(int index) {
        return "__dc_agg_" + index;
    }

    private static List<String> extractGroupByColumns(SqlSelect select) {
        if (select.getGroup() == null || select.getGroup().isEmpty()) {
            return List.of();
        }
        List<String> columns = new ArrayList<>();
        for (SqlNode groupItem : select.getGroup()) {
            columns.add(columnName(groupItem));
        }
        return columns;
    }

    private static AggregateSpec toAggregateSpec(SqlNode item, int index) {
        SqlCall call = (SqlCall) AggregateSqlSupport.unwrapAlias(item);
        AggregateFunction function = AggregateSqlSupport.toAggregateFunction(call);
        String inputColumn = extractInputColumn(call);
        String outputName = outputName(item, function, inputColumn, index);
        return new AggregateSpec(outputName, mergeColumnName(index), function, inputColumn);
    }

    private static SqlNode unwrapAlias(SqlNode item) {
        return AggregateSqlSupport.unwrapAlias(item);
    }

    private static String extractInputColumn(SqlCall call) {
        if (call.getOperandList().isEmpty()) {
            return null;
        }
        SqlNode operand = call.operand(0);
        if (operand instanceof SqlIdentifier identifier && identifier.isStar()) {
            return null;
        }
        return columnName(operand);
    }

    private static String outputName(SqlNode item, AggregateFunction function, String inputColumn, int index) {
        if (item instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.AS) {
            SqlNode aliasNode = call.operand(1);
            if (aliasNode instanceof SqlIdentifier alias) {
                return alias.getSimple();
            }
        }
        if (function == AggregateFunction.COUNT && inputColumn == null) {
            return "count";
        }
        if (inputColumn != null) {
            return function.name().toLowerCase() + "_" + inputColumn;
        }
        return function.name().toLowerCase() + "_" + index;
    }

    private static String columnName(SqlNode node) {
        if (node instanceof SqlIdentifier identifier) {
            return identifier.getSimple();
        }
        throw new IllegalArgumentException("Expected column identifier but found: " + node);
    }

    private static boolean isAggregate(SqlNode node) {
        return AggregateSqlSupport.isAggregateExpression(node);
    }
}
