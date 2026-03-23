package io.duckcluster.common.planner;

import io.duckcluster.common.model.AggregateSpec;
import io.duckcluster.common.model.QueryAnalysis;
import io.duckcluster.common.model.TableShardConfig;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
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

public final class FragmentSqlGenerator {
    private static final SqlDialect DIALECT = DuckDBSqlDialect.DEFAULT;

    private FragmentSqlGenerator() {}

    public static String generate(SqlSelect select, TableShardConfig tableConfig, int shardId, QueryAnalysis analysis) {
        SqlSelect fragmentSelect = (SqlSelect) select.clone(SqlParserPos.ZERO);

        String catalogName = tableConfig.tableName() + "_shard" + shardId;
        SqlIdentifier qualifiedTable = new SqlIdentifier(
                List.of(catalogName, tableConfig.tableName()), SqlParserPos.ZERO);
        fragmentSelect.setFrom(qualifiedTable);

        if (analysis.hasAggregates()) {
            fragmentSelect.setSelectList(rewriteAggregates(select.getSelectList(), analysis.aggregates()));
        }

        return toSql(fragmentSelect);
    }

    private static SqlNodeList rewriteAggregates(SqlNodeList selectList, List<AggregateSpec> aggregates) {
        List<SqlNode> rewritten = new ArrayList<>();
        int aggregateIndex = 0;
        for (SqlNode item : selectList) {
            if (AggregateSqlSupport.isAggregateExpression(item)) {
                AggregateSpec aggregate = aggregates.get(aggregateIndex++);
                SqlCall aggregateCall = (SqlCall) AggregateSqlSupport.unwrapAlias(item);
                SqlNode alias = new SqlIdentifier(aggregate.mergeColumnName(), SqlParserPos.ZERO);
                rewritten.add(SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, aggregateCall, alias));
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
