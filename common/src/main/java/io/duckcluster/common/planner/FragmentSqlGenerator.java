package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateFunction;
import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.OrderByClause;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TopKSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class FragmentSqlGenerator {
    private static final SqlDialect DIALECT = DuckDBSqlDialect.DEFAULT;

    private FragmentSqlGenerator() {}

    public static String generate(SqlSelect select, int shardId, QueryAnalysis analysis,
                                   String drivingTable, Map<String, Integer> broadcastShardCounts,
                                   Map<String, Integer> catalogTableShardCounts, TopKSpec topK) {
        SqlSelect fragmentSelect = (SqlSelect) select.clone(SqlParserPos.ZERO);

        FromScope outerFromScope = FromScope.of(select.getFrom());

        SqlNode rewrittenFrom = rewriteFrom(
                select.getFrom(), shardId, drivingTable, broadcastShardCounts, catalogTableShardCounts,
                TableRewriteMode.FRAGMENT, outerFromScope);
        fragmentSelect.setFrom(rewrittenFrom);

        if (select.getWhere() != null) {
            fragmentSelect.setWhere(rewritePredicateTree(
                    select.getWhere(), shardId, drivingTable, broadcastShardCounts, catalogTableShardCounts,
                    outerFromScope));
        }
        if (select.getHaving() != null) {
            fragmentSelect.setHaving(rewritePredicateTree(
                    select.getHaving(), shardId, drivingTable, broadcastShardCounts, catalogTableShardCounts,
                    outerFromScope));
        }

        if (analysis.hasAggregates()) {
            fragmentSelect.setSelectList(rewriteSelectList(select.getSelectList(), analysis));
            if (analysis.hasDistinctCountAggregates()) {
                expandGroupByForDistinctCount(fragmentSelect, analysis);
            }
        }

        // Partial shard GROUP BY results are merged globally; ORDER BY/LIMIT belong on the coordinator.
        TopKSpec fragmentTopK = analysis.groupByColumns().isEmpty() ? topK : TopKSpec.none();
        return appendTopK(toSql(fragmentSelect), fragmentTopK);
    }

    private static String appendTopK(String sql, TopKSpec topK) {
        if (!topK.hasTopK()) {
            return sql;
        }
        StringBuilder builder = new StringBuilder(sql);
        if (topK.hasOrderBy()) {
            builder.append(" ORDER BY ");
            for (int i = 0; i < topK.orderBy().size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                OrderByClause clause = topK.orderBy().get(i);
                builder.append(quoteIdentifier(clause.column()));
                if (clause.descending()) {
                    builder.append(" DESC");
                }
            }
        }
        if (topK.hasLimit()) {
            builder.append(" LIMIT ").append(topK.limit());
        }
        return builder.toString();
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private enum TableRewriteMode {
        /** Main query FROM: driving shard + broadcast UNION ALL. */
        FRAGMENT,
        /** Uncorrelated subquery: global UNION ALL across shards. */
        GLOBAL_SUBQUERY,
        /** Correlated EXISTS/NOT EXISTS: co-located shard slice. */
        CO_LOCATED_SUBQUERY
    }

    private static SqlNode rewriteFrom(SqlNode from, int shardId,
                                        String drivingTable, Map<String, Integer> broadcastShardCounts,
                                        Map<String, Integer> catalogTableShardCounts, TableRewriteMode mode,
                                        FromScope enclosingScope) {
        if (from instanceof SqlIdentifier identifier) {
            return rewriteTableIdentifier(
                    identifier, shardId, drivingTable, broadcastShardCounts, catalogTableShardCounts, mode);
        }
        if (from instanceof SqlSelect subquery) {
            return rewriteEmbeddedSelect(subquery, shardId, drivingTable, broadcastShardCounts,
                    catalogTableShardCounts, mode, enclosingScope);
        }
        if (from instanceof SqlJoin join) {
            SqlNode left = rewriteFrom(join.getLeft(), shardId, drivingTable, broadcastShardCounts,
                    catalogTableShardCounts, mode, enclosingScope);
            SqlNode right = rewriteFrom(join.getRight(), shardId, drivingTable, broadcastShardCounts,
                    catalogTableShardCounts, mode, enclosingScope);
            return new SqlJoin(
                    join.getParserPosition(),
                    left,
                    join.isNaturalNode(),
                    join.getJoinTypeNode(),
                    right,
                    join.getConditionTypeNode(),
                    join.getCondition());
        }
        if (from instanceof SqlBasicCall call && (call.getKind() == SqlKind.AS || call.getKind() == SqlKind.UNION)) {
            if (call.getKind() == SqlKind.UNION) {
                SqlNode rewrittenUnion = rewriteUnionOperands(
                        call, shardId, drivingTable, broadcastShardCounts, catalogTableShardCounts, mode,
                        enclosingScope);
                return rewrittenUnion;
            }
            SqlNode inner = call.operand(0);
            SqlNode alias = call.operand(1);
            String tableName = TableNameSupport.baseTableName(inner);
            if (mode == TableRewriteMode.FRAGMENT && tableName != null
                    && broadcastShardCounts.containsKey(tableName.toLowerCase())) {
                SqlNode unionAll = buildUnionAll(tableName.toLowerCase(), broadcastShardCounts.get(tableName.toLowerCase()));
                return SqlStdOperatorTable.AS.createCall(call.getParserPosition(), unionAll, alias);
            }
            SqlNode rewrittenInner = rewriteFrom(inner, shardId, drivingTable, broadcastShardCounts,
                    catalogTableShardCounts, mode, enclosingScope);
            return recreateAsCall(call, rewrittenInner, alias);
        }
        return from;
    }

    private static SqlNode rewriteFrom(SqlNode from, int shardId,
                                        String drivingTable, Map<String, Integer> broadcastShardCounts,
                                        Map<String, Integer> catalogTableShardCounts, TableRewriteMode mode) {
        return rewriteFrom(from, shardId, drivingTable, broadcastShardCounts, catalogTableShardCounts, mode,
                FromScope.ofTables(List.of()));
    }

    private static SqlNode rewriteUnionOperands(
            SqlBasicCall union,
            int shardId,
            String drivingTable,
            Map<String, Integer> broadcastShardCounts,
            Map<String, Integer> catalogTableShardCounts,
            TableRewriteMode mode,
            FromScope enclosingScope) {
        List<SqlNode> rewrittenOperands = new ArrayList<>();
        for (SqlNode operand : union.getOperandList()) {
            rewrittenOperands.add(rewriteFrom(operand, shardId, drivingTable, broadcastShardCounts,
                    catalogTableShardCounts, mode, enclosingScope));
        }
        return union.getOperator().createCall(union.getParserPosition(), rewrittenOperands);
    }

    private static SqlSelect rewriteEmbeddedSelect(
            SqlSelect select,
            int shardId,
            String drivingTable,
            Map<String, Integer> broadcastShardCounts,
            Map<String, Integer> catalogTableShardCounts,
            TableRewriteMode mode,
            FromScope enclosingScope) {
        SqlSelect rewritten = (SqlSelect) select.clone(SqlParserPos.ZERO);
        FromScope scope = enclosingScope.union(FromScope.of(select.getFrom()));
        if (select.getFrom() != null) {
            rewritten.setFrom(rewriteFrom(select.getFrom(), shardId, drivingTable, broadcastShardCounts,
                    catalogTableShardCounts, mode, scope));
        }
        if (select.getWhere() != null) {
            rewritten.setWhere(rewritePredicateTree(select.getWhere(), shardId, drivingTable,
                    broadcastShardCounts, catalogTableShardCounts, scope));
        }
        if (select.getHaving() != null) {
            rewritten.setHaving(rewritePredicateTree(select.getHaving(), shardId, drivingTable,
                    broadcastShardCounts, catalogTableShardCounts, scope));
        }
        return rewritten;
    }

    private static SqlNode recreateAsCall(SqlBasicCall original, SqlNode rewrittenInner, SqlNode alias) {
        if (original.operandCount() == 2) {
            return SqlStdOperatorTable.AS.createCall(original.getParserPosition(), rewrittenInner, alias);
        }
        List<SqlNode> operands = new ArrayList<>(original.operandCount());
        operands.add(rewrittenInner);
        operands.add(alias);
        for (int i = 2; i < original.operandCount(); i++) {
            operands.add(original.operand(i));
        }
        return original.getOperator().createCall(original.getParserPosition(), operands);
    }

    private static SqlNode rewritePredicateTree(
            SqlNode node,
            int shardId,
            String drivingTable,
            Map<String, Integer> broadcastShardCounts,
            Map<String, Integer> catalogTableShardCounts,
            FromScope enclosingScope) {
        return node.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlCall call) {
                if (SubqueryAnalyzer.isPredicateSubquery(call)) {
                    SqlSelect subquery = SubqueryAnalyzer.operandSubquery(call);
                    boolean correlated = SubqueryAnalyzer.isCorrelated(subquery, enclosingScope);
                    boolean coLocated = correlated
                            && SubqueryAnalyzer.isExistsSubquery(call)
                            && SubqueryAnalyzer.canCoLocateSubquery(
                                    subquery, drivingTable, catalogTableShardCounts);
                    SqlSelect rewrittenSubquery = rewriteSubqueryBody(
                            subquery, shardId, drivingTable, broadcastShardCounts, catalogTableShardCounts,
                            coLocated, enclosingScope);
                    return SubqueryAnalyzer.replaceSubqueryOperand(call, rewrittenSubquery);
                }
                return super.visit(call);
            }
        });
    }

    private static SqlSelect rewriteSubqueryBody(
            SqlSelect select,
            int shardId,
            String drivingTable,
            Map<String, Integer> broadcastShardCounts,
            Map<String, Integer> catalogTableShardCounts,
            boolean coLocated,
            FromScope enclosingScope) {
        TableRewriteMode mode = coLocated ? TableRewriteMode.CO_LOCATED_SUBQUERY : TableRewriteMode.GLOBAL_SUBQUERY;
        FromScope subqueryScope = enclosingScope.union(FromScope.of(select.getFrom()));
        SqlSelect rewritten = (SqlSelect) select.clone(SqlParserPos.ZERO);

        if (select.getFrom() != null) {
            rewritten.setFrom(rewriteFrom(select.getFrom(), shardId, drivingTable, broadcastShardCounts,
                    catalogTableShardCounts, mode, subqueryScope));
        }
        if (select.getWhere() != null) {
            rewritten.setWhere(rewritePredicateTree(select.getWhere(), shardId, drivingTable,
                    broadcastShardCounts, catalogTableShardCounts, subqueryScope));
        }
        if (select.getHaving() != null) {
            rewritten.setHaving(rewritePredicateTree(select.getHaving(), shardId, drivingTable,
                    broadcastShardCounts, catalogTableShardCounts, subqueryScope));
        }
        return rewritten;
    }

    private static SqlNode rewriteTableIdentifier(SqlIdentifier identifier, int shardId,
                                                   String drivingTable, Map<String, Integer> broadcastShardCounts,
                                                   Map<String, Integer> catalogTableShardCounts,
                                                   TableRewriteMode mode) {
        String tableName = TableNameSupport.baseTableName(identifier);
        if (tableName == null) {
            return identifier;
        }
        String tableNameLower = tableName.toLowerCase();

        if (mode == TableRewriteMode.GLOBAL_SUBQUERY && catalogTableShardCounts.containsKey(tableNameLower)) {
            int tableShardCount = catalogTableShardCounts.get(tableNameLower);
            if (tableShardCount == 1) {
                return new SqlIdentifier(List.of(tableNameLower + "_shard0", tableNameLower), SqlParserPos.ZERO);
            }
            return buildUnionAll(tableNameLower, tableShardCount);
        }

        if (mode == TableRewriteMode.CO_LOCATED_SUBQUERY && catalogTableShardCounts.containsKey(tableNameLower)) {
            int tableShardCount = catalogTableShardCounts.get(tableNameLower);
            if (tableShardCount == 1) {
                return new SqlIdentifier(List.of(tableNameLower + "_shard0", tableNameLower), SqlParserPos.ZERO);
            }
            String catalogName = tableNameLower + "_shard" + shardId;
            return new SqlIdentifier(List.of(catalogName, tableNameLower), SqlParserPos.ZERO);
        }

        if (tableNameLower.equals(drivingTable.toLowerCase())) {
            String catalogName = tableNameLower + "_shard" + shardId;
            return new SqlIdentifier(List.of(catalogName, tableNameLower), SqlParserPos.ZERO);
        } else if (broadcastShardCounts.containsKey(tableNameLower)) {
            return buildUnionAll(tableNameLower, broadcastShardCounts.get(tableNameLower));
        } else if (catalogTableShardCounts.containsKey(tableNameLower)) {
            int tableShardCount = catalogTableShardCounts.get(tableNameLower);
            if (tableShardCount == 1) {
                return new SqlIdentifier(List.of(tableNameLower + "_shard0", tableNameLower), SqlParserPos.ZERO);
            }
            String catalogName = tableNameLower + "_shard" + shardId;
            return new SqlIdentifier(List.of(catalogName, tableNameLower), SqlParserPos.ZERO);
        } else {
            return identifier;
        }
    }

    private static SqlNode buildUnionAll(String tableName, int shardCount) {
        SqlNode result = buildShardSelect(tableName, 0);
        for (int i = 1; i < shardCount; i++) {
            SqlNode next = buildShardSelect(tableName, i);
            result = SqlStdOperatorTable.UNION_ALL.createCall(SqlParserPos.ZERO, result, next);
        }
        return result;
    }

    private static SqlNode buildShardSelect(String tableName, int shardId) {
        SqlIdentifier qualifiedTable = new SqlIdentifier(
                List.of(tableName + "_shard" + shardId, tableName), SqlParserPos.ZERO);
        return new SqlSelect(
                SqlParserPos.ZERO,
                null,
                SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
                qualifiedTable,
                null, null, null, null, null, null, null, SqlNodeList.EMPTY);
    }

    private static String extractTableName(SqlNode node) {
        return TableNameSupport.baseTableName(node);
    }

    private static SqlNodeList rewriteSelectList(SqlNodeList selectList, QueryAnalysis analysis) {
        if (!analysis.hasComputedOutputs()) {
            return rewriteAggregates(selectList, analysis.aggregates());
        }

        List<SqlNode> rewritten = new ArrayList<>();
        int aggregateIndex = 0;
        for (SqlNode item : selectList) {
            if (AggregateSqlSupport.isAggregateExpression(item)) {
                SqlCall aggregateCall = (SqlCall) AggregateSqlSupport.unwrapAlias(item);
                AggregateFunction function = AggregateSqlSupport.toAggregateFunction(aggregateCall);
                if (function == AggregateFunction.AVG) {
                    SqlNode operand = aggregateCall.getOperandList().isEmpty() ? null : aggregateCall.operand(0);
                    AggregateSpec sumSpec = analysis.aggregates().get(aggregateIndex++);
                    AggregateSpec countSpec = analysis.aggregates().get(aggregateIndex++);
                    SqlNode sumCall = SqlStdOperatorTable.SUM.createCall(aggregateCall.getParserPosition(), operand);
                    SqlNode countCall = SqlStdOperatorTable.COUNT.createCall(aggregateCall.getParserPosition(), operand);
                    rewritten.add(SqlStdOperatorTable.AS.createCall(
                            aggregateCall.getParserPosition(),
                            sumCall,
                            new SqlIdentifier(sumSpec.mergeColumnName(), SqlParserPos.ZERO)));
                    rewritten.add(SqlStdOperatorTable.AS.createCall(
                            aggregateCall.getParserPosition(),
                            countCall,
                            new SqlIdentifier(countSpec.mergeColumnName(), SqlParserPos.ZERO)));
                } else {
                    AggregateSpec aggregate = analysis.aggregates().get(aggregateIndex++);
                    if (aggregate.part() == AggregateSpec.AggregatePart.DISTINCT_COUNT) {
                        SqlNode operand = aggregateCall.getOperandList().isEmpty() ? null : aggregateCall.operand(0);
                        SqlNode alias = new SqlIdentifier(aggregate.mergeColumnName(), SqlParserPos.ZERO);
                        rewritten.add(SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, operand, alias));
                    } else {
                        SqlNode alias = new SqlIdentifier(aggregate.mergeColumnName(), SqlParserPos.ZERO);
                        rewritten.add(SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, aggregateCall, alias));
                    }
                }
            } else if (NestedAggregateExtractor.containsNestedAggregate(item)) {
                SqlNode expression = AggregateSqlSupport.unwrapAlias(item);
                for (SqlCall call : NestedAggregateExtractor.collectAggregateCalls(expression)) {
                    AggregateSpec aggregate = analysis.aggregates().get(aggregateIndex++);
                    SqlNode alias = new SqlIdentifier(aggregate.mergeColumnName(), SqlParserPos.ZERO);
                    rewritten.add(SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, call, alias));
                }
            } else {
                rewritten.add(item);
            }
        }
        return new SqlNodeList(rewritten, SqlParserPos.ZERO);
    }

    private static SqlNodeList rewriteAggregates(SqlNodeList selectList, List<AggregateSpec> aggregates) {
        List<SqlNode> rewritten = new ArrayList<>();
        int aggregateIndex = 0;
        for (SqlNode item : selectList) {
            if (AggregateSqlSupport.isAggregateExpression(item)) {
                SqlCall aggregateCall = (SqlCall) AggregateSqlSupport.unwrapAlias(item);
                AggregateFunction function = AggregateSqlSupport.toAggregateFunction(aggregateCall);
                if (function == AggregateFunction.AVG) {
                    SqlNode operand = aggregateCall.getOperandList().isEmpty() ? null : aggregateCall.operand(0);
                    AggregateSpec sumSpec = aggregates.get(aggregateIndex++);
                    AggregateSpec countSpec = aggregates.get(aggregateIndex++);
                    SqlNode sumCall = SqlStdOperatorTable.SUM.createCall(aggregateCall.getParserPosition(), operand);
                    SqlNode countCall = SqlStdOperatorTable.COUNT.createCall(aggregateCall.getParserPosition(), operand);
                    rewritten.add(SqlStdOperatorTable.AS.createCall(
                            aggregateCall.getParserPosition(),
                            sumCall,
                            new SqlIdentifier(sumSpec.mergeColumnName(), SqlParserPos.ZERO)));
                    rewritten.add(SqlStdOperatorTable.AS.createCall(
                            aggregateCall.getParserPosition(),
                            countCall,
                            new SqlIdentifier(countSpec.mergeColumnName(), SqlParserPos.ZERO)));
                } else {
                    AggregateSpec aggregate = aggregates.get(aggregateIndex++);
                    if (aggregate.part() == AggregateSpec.AggregatePart.DISTINCT_COUNT) {
                        SqlNode operand = aggregateCall.getOperandList().isEmpty() ? null : aggregateCall.operand(0);
                        SqlNode alias = new SqlIdentifier(aggregate.mergeColumnName(), SqlParserPos.ZERO);
                        rewritten.add(SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, operand, alias));
                    } else {
                        SqlNode alias = new SqlIdentifier(aggregate.mergeColumnName(), SqlParserPos.ZERO);
                        rewritten.add(SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, aggregateCall, alias));
                    }
                }
            } else {
                rewritten.add(item);
            }
        }
        return new SqlNodeList(rewritten, SqlParserPos.ZERO);
    }

    private static void expandGroupByForDistinctCount(SqlSelect select, QueryAnalysis analysis) {
        SqlNodeList group = select.getGroup();
        List<SqlNode> expanded = new ArrayList<>();
        Set<String> existing = new HashSet<>();
        if (group != null) {
            for (SqlNode groupItem : group) {
                expanded.add(groupItem);
                existing.add(groupColumnName(groupItem));
            }
        }
        for (AggregateSpec aggregate : analysis.aggregates()) {
            if (aggregate.part() != AggregateSpec.AggregatePart.DISTINCT_COUNT) {
                continue;
            }
            if (aggregate.inputColumn() != null && !existing.contains(aggregate.inputColumn())) {
                expanded.add(new SqlIdentifier(aggregate.inputColumn(), SqlParserPos.ZERO));
                existing.add(aggregate.inputColumn());
            } else if (aggregate.hasInputExpression() && aggregate.inputExpression() != null) {
                expanded.add(aggregate.inputExpression());
            }
        }
        select.setGroupBy(new SqlNodeList(expanded, SqlParserPos.ZERO));
    }

    private static String groupColumnName(SqlNode node) {
        if (node instanceof SqlIdentifier identifier) {
            List<String> names = identifier.names;
            return names.get(names.size() - 1);
        }
        return node.toString();
    }

    private static String toSql(SqlNode node) {
        SqlPrettyWriter writer = new SqlPrettyWriter(DIALECT);
        node.unparse(writer, 0, 0);
        String sql = writer.toSqlString().getSql();
        if (sql.endsWith(";")) {
            return sql.substring(0, sql.length() - 1);
        }
        return sql;
    }
}
