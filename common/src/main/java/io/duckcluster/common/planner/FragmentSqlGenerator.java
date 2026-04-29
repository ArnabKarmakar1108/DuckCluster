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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class FragmentSqlGenerator {
    private static final SqlDialect DIALECT = DuckDBSqlDialect.DEFAULT;

    private FragmentSqlGenerator() {}

    public static String generate(SqlSelect select, int shardId, QueryAnalysis analysis,
                                   String drivingTable, Map<String, Integer> broadcastShardCounts,
                                   TopKSpec topK) {
        SqlSelect fragmentSelect = (SqlSelect) select.clone(SqlParserPos.ZERO);

        SqlNode rewrittenFrom = rewriteFrom(select.getFrom(), shardId, drivingTable, broadcastShardCounts);
        fragmentSelect.setFrom(rewrittenFrom);

        if (analysis.hasAggregates()) {
            fragmentSelect.setSelectList(rewriteAggregates(select.getSelectList(), analysis.aggregates()));
        }

        return appendTopK(toSql(fragmentSelect), topK);
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

    private static SqlNode rewriteFrom(SqlNode from, int shardId,
                                        String drivingTable, Map<String, Integer> broadcastShardCounts) {
        if (from instanceof SqlIdentifier identifier) {
            return rewriteTableIdentifier(identifier, shardId, drivingTable, broadcastShardCounts);
        }
        if (from instanceof SqlJoin join) {
            SqlNode left = rewriteFrom(join.getLeft(), shardId, drivingTable, broadcastShardCounts);
            SqlNode right = rewriteFrom(join.getRight(), shardId, drivingTable, broadcastShardCounts);
            return new SqlJoin(
                    join.getParserPosition(),
                    left,
                    join.isNaturalNode(),
                    join.getJoinTypeNode(),
                    right,
                    join.getConditionTypeNode(),
                    join.getCondition());
        }
        if (from instanceof SqlBasicCall call && call.getKind() == SqlKind.AS) {
            SqlNode inner = call.operand(0);
            SqlNode alias = call.operand(1);
            String tableName = extractTableName(inner);
            if (tableName != null && broadcastShardCounts.containsKey(tableName.toLowerCase())) {
                SqlNode unionAll = buildUnionAll(tableName.toLowerCase(), broadcastShardCounts.get(tableName.toLowerCase()));
                return SqlStdOperatorTable.AS.createCall(call.getParserPosition(), unionAll, alias);
            }
            SqlNode rewrittenInner = rewriteFrom(inner, shardId, drivingTable, broadcastShardCounts);
            return SqlStdOperatorTable.AS.createCall(call.getParserPosition(), rewrittenInner, alias);
        }
        return from;
    }

    private static SqlNode rewriteTableIdentifier(SqlIdentifier identifier, int shardId,
                                                   String drivingTable, Map<String, Integer> broadcastShardCounts) {
        if (identifier.names.size() != 1) {
            return identifier;
        }
        String tableName = identifier.getSimple();
        String tableNameLower = tableName.toLowerCase();

        if (tableNameLower.equals(drivingTable.toLowerCase())) {
            String catalogName = tableNameLower + "_shard" + shardId;
            return new SqlIdentifier(List.of(catalogName, tableNameLower), SqlParserPos.ZERO);
        } else if (broadcastShardCounts.containsKey(tableNameLower)) {
            return buildUnionAll(tableNameLower, broadcastShardCounts.get(tableNameLower));
        } else {
            return new SqlIdentifier(List.of(tableNameLower, tableNameLower), SqlParserPos.ZERO);
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
        if (node instanceof SqlIdentifier id && id.names.size() == 1) {
            return id.getSimple();
        }
        return null;
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
                    SqlNode alias = new SqlIdentifier(aggregate.mergeColumnName(), SqlParserPos.ZERO);
                    rewritten.add(SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, aggregateCall, alias));
                }
            } else {
                rewritten.add(item);
            }
        }
        return new SqlNodeList(rewritten, SqlParserPos.ZERO);
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
