package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.ComputedOutputSpec;
import io.duckcluster.common.model.MergeStrategyType;
import io.duckcluster.common.model.QueryAnalysis;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;

public final class QueryAnalysisExtractor {
    private QueryAnalysisExtractor() {}

    public static QueryAnalysis extract(SqlSelect select, MergeStrategyType mergeStrategy) {
        if (mergeStrategy == MergeStrategyType.CONCATENATE) {
            return QueryAnalysis.empty();
        }
        if (mergeStrategy == MergeStrategyType.TOP_K) {
            List<String> outputColumnNames = new ArrayList<>();
            for (SqlNode item : select.getSelectList()) {
                outputColumnNames.add(selectOutputName(item));
            }
            return new QueryAnalysis(List.of(), List.of(), outputColumnNames);
        }

        List<String> groupByColumns = extractGroupByColumns(select);
        List<AggregateSpec> aggregates = new ArrayList<>();
        List<String> outputColumnNames = new ArrayList<>();
        List<ComputedOutputSpec> computedOutputs = new ArrayList<>();

        int aggregateIndex = 0;
        for (SqlNode item : select.getSelectList()) {
            if (isAggregate(item)) {
                List<AggregateSpec> specs = toAggregateSpecs(item, aggregateIndex++);
                aggregates.addAll(specs);
                outputColumnNames.add(specs.get(0).outputName());
            } else if (NestedAggregateExtractor.containsNestedAggregate(item)) {
                NestedAggregateExtractor.Analysis nested = NestedAggregateExtractor.analyze(item, aggregateIndex);
                aggregates.addAll(nested.aggregates());
                outputColumnNames.add(nested.outputName());
                computedOutputs.add(nested.computedOutput());
                aggregateIndex = nested.nextAggregateIndex();
            } else if (mergeStrategy == MergeStrategyType.GROUP_BY_MERGE) {
                outputColumnNames.add(selectOutputName(item));
            }
        }

        return new QueryAnalysis(groupByColumns, aggregates, outputColumnNames, computedOutputs);
    }

    public static QueryAnalysis withMergeColumnNames(QueryAnalysis analysis) {
        List<AggregateSpec> rewritten = new ArrayList<>(analysis.aggregates().size());
        int mergeIndex = 0;
        for (int i = 0; i < analysis.aggregates().size(); ) {
            AggregateSpec aggregate = analysis.aggregates().get(i);
            if (aggregate.part() == AggregateSpec.AggregatePart.AVG_SUM) {
                AggregateSpec count = analysis.aggregates().get(i + 1);
                rewritten.add(new AggregateSpec(
                        aggregate.outputName(),
                        mergeColumnName(mergeIndex) + "_sum",
                        aggregate.function(),
                        aggregate.inputColumn(),
                        aggregate.inputExpression(),
                        AggregateSpec.AggregatePart.AVG_SUM));
                rewritten.add(new AggregateSpec(
                        count.outputName(),
                        mergeColumnName(mergeIndex) + "_cnt",
                        count.function(),
                        count.inputColumn(),
                        count.inputExpression(),
                        AggregateSpec.AggregatePart.AVG_COUNT));
                mergeIndex++;
                i += 2;
            } else if (aggregate.part() == AggregateSpec.AggregatePart.DISTINCT_COUNT) {
                rewritten.add(new AggregateSpec(
                        aggregate.outputName(),
                        distinctMergeColumnName(mergeIndex),
                        aggregate.function(),
                        aggregate.inputColumn(),
                        aggregate.inputExpression(),
                        AggregateSpec.AggregatePart.DISTINCT_COUNT));
                mergeIndex++;
                i++;
            } else {
                rewritten.add(new AggregateSpec(
                        aggregate.outputName(),
                        mergeColumnName(mergeIndex),
                        aggregate.function(),
                        aggregate.inputColumn(),
                        aggregate.inputExpression(),
                        aggregate.part()));
                mergeIndex++;
                i++;
            }
        }
        return new QueryAnalysis(analysis.groupByColumns(), rewritten, analysis.outputColumnNames(), analysis.computedOutputs());
    }

    static String mergeColumnName(int index) {
        return "__dc_agg_" + index;
    }

    static String distinctMergeColumnName(int index) {
        return "__dc_distinct_" + index;
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

    private static List<AggregateSpec> toAggregateSpecs(SqlNode item, int index) {
        SqlCall call = (SqlCall) AggregateSqlSupport.unwrapAlias(item);
        AggregateFunction function = AggregateSqlSupport.toAggregateFunction(call);
        AggregateOperand operand = extractAggregateOperand(call);
        String outputName = outputName(item, function, operand, index);
        if (function == AggregateFunction.AVG) {
            return List.of(
                    new AggregateSpec(
                            outputName,
                            mergeColumnName(index) + "_sum",
                            AggregateFunction.SUM,
                            operand.inputColumn(),
                            operand.inputExpression(),
                            AggregateSpec.AggregatePart.AVG_SUM),
                    new AggregateSpec(
                            outputName,
                            mergeColumnName(index) + "_cnt",
                            AggregateFunction.COUNT,
                            operand.inputColumn(),
                            operand.inputExpression(),
                            AggregateSpec.AggregatePart.AVG_COUNT));
        }
        if (function == AggregateFunction.COUNT && AggregateSqlSupport.isCountDistinct(call)) {
            return List.of(new AggregateSpec(
                    outputName,
                    mergeColumnName(index),
                    function,
                    operand.inputColumn(),
                    operand.inputExpression(),
                    AggregateSpec.AggregatePart.DISTINCT_COUNT));
        }
        return List.of(new AggregateSpec(
                outputName,
                mergeColumnName(index),
                function,
                operand.inputColumn(),
                operand.inputExpression()));
    }

    private static AggregateOperand extractAggregateOperand(SqlCall call) {
        if (call.getOperandList().isEmpty()) {
            return new AggregateOperand(null, null);
        }
        SqlNode operand = call.operand(0);
        if (operand instanceof SqlIdentifier identifier && identifier.isStar()) {
            return new AggregateOperand(null, null);
        }
        if (operand instanceof SqlIdentifier identifier) {
            return new AggregateOperand(columnNameFromIdentifier(identifier), null);
        }
        return new AggregateOperand(null, operand);
    }

    private static String outputName(SqlNode item, AggregateFunction function, AggregateOperand operand, int index) {
        if (item instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.AS) {
            SqlNode aliasNode = call.operand(1);
            if (aliasNode instanceof SqlIdentifier alias) {
                return alias.getSimple();
            }
        }
        if (function == AggregateFunction.COUNT && operand.inputColumn() == null && operand.inputExpression() == null) {
            return "count";
        }
        if (operand.inputColumn() != null) {
            return function.name().toLowerCase() + "(" + operand.inputColumn() + ")";
        }
        return function.name().toLowerCase() + "_" + index;
    }

    private static String selectOutputName(SqlNode item) {
        if (item instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.AS) {
            SqlNode aliasNode = call.operand(1);
            if (aliasNode instanceof SqlIdentifier alias) {
                return alias.getSimple();
            }
        }
        return columnName(item);
    }

    private static String columnName(SqlNode node) {
        if (node instanceof SqlIdentifier identifier) {
            return columnNameFromIdentifier(identifier);
        }
        throw new IllegalArgumentException("Expected column identifier but found: " + node);
    }

    private static String columnNameFromIdentifier(SqlIdentifier identifier) {
        List<String> names = identifier.names;
        return names.get(names.size() - 1);
    }

    private static boolean isAggregate(SqlNode node) {
        return AggregateSqlSupport.isAggregateExpression(node);
    }

    private record AggregateOperand(String inputColumn, SqlNode inputExpression) {}
}
