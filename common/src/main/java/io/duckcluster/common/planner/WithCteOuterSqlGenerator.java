package io.duckcluster.common.planner;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlShuttle;

final class WithCteOuterSqlGenerator {
    private static final String MERGE_TEMP = "__merge_temp";
    private static final SqlDialect DIALECT = DuckDBSqlDialect.DEFAULT;

    private WithCteOuterSqlGenerator() {}

    static String generate(SqlNode outer, String cteAlias) {
        SqlNode rewritten = outer.accept(new CteReplacer(cteAlias.toLowerCase()));
        return toSql(rewritten);
    }

    private static final class CteReplacer extends SqlShuttle {
        private final String cteAlias;

        private CteReplacer(String cteAlias) {
            this.cteAlias = cteAlias;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            if (id.names.size() == 1 && id.getSimple().equalsIgnoreCase(cteAlias)) {
                SqlIdentifier temp = new SqlIdentifier(MERGE_TEMP, SqlParserPos.ZERO);
                SqlIdentifier alias = new SqlIdentifier(cteAlias, SqlParserPos.ZERO);
                return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, temp, alias);
            }
            return id;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call.getKind() == SqlKind.AS) {
                SqlBasicCall asCall = (SqlBasicCall) call;
                SqlNode inner = asCall.operand(0);
                if (inner instanceof SqlIdentifier id && id.getSimple().equalsIgnoreCase(cteAlias)) {
                    SqlIdentifier temp = new SqlIdentifier(MERGE_TEMP, SqlParserPos.ZERO);
                    return recreateAsCall(asCall, temp, asCall.operand(1));
                }
            }
            return super.visit(call);
        }
    }

    private static SqlNode recreateAsCall(SqlBasicCall original, SqlNode rewrittenInner, SqlNode alias) {
        if (original.operandCount() == 2) {
            return SqlStdOperatorTable.AS.createCall(original.getParserPosition(), rewrittenInner, alias);
        }
        java.util.List<SqlNode> operands = new java.util.ArrayList<>(original.operandCount());
        operands.add(rewrittenInner);
        operands.add(alias);
        for (int i = 2; i < original.operandCount(); i++) {
            operands.add(original.operand(i));
        }
        return original.getOperator().createCall(original.getParserPosition(), operands);
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
