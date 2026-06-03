package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Inlines WITH/CTE definitions by substituting CTE names with their subqueries. */
final class WithClauseExpander {
    private WithClauseExpander() {}

    static SqlNode expand(SqlNode parsed) {
        if (!(parsed instanceof SqlWith with)) {
            return parsed;
        }
        Map<String, SqlNode> ctes = new LinkedHashMap<>();
        for (SqlNode item : with.withList) {
            if (!(item instanceof SqlWithItem withItem)) {
                continue;
            }
            String name = withItem.name.getSimple().toLowerCase();
            ctes.put(name, withItem.query);
        }
        return substitute(with.body, ctes);
    }

    static Map<String, SqlSelect> cteQueries(SqlWith with) {
        Map<String, SqlSelect> ctes = new LinkedHashMap<>();
        for (SqlNode item : with.withList) {
            if (!(item instanceof SqlWithItem withItem) || !(withItem.query instanceof SqlSelect select)) {
                continue;
            }
            ctes.put(withItem.name.getSimple().toLowerCase(), select);
        }
        return Map.copyOf(ctes);
    }

    private static SqlNode substitute(SqlNode node, Map<String, SqlNode> ctes) {
        return node.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlIdentifier id) {
                if (id.names.size() == 1 && ctes.containsKey(id.getSimple().toLowerCase())) {
                    SqlNode cteQuery = ctes.get(id.getSimple().toLowerCase());
                    SqlIdentifier alias = new SqlIdentifier(id.getSimple(), SqlParserPos.ZERO);
                    return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, cteQuery, alias);
                }
                return id;
            }

            @Override
            public SqlNode visit(SqlCall call) {
                if (call.getKind() == SqlKind.AS) {
                    SqlBasicCall asCall = (SqlBasicCall) call;
                    SqlNode inner = asCall.operand(0);
                    if (inner instanceof SqlIdentifier id && ctes.containsKey(id.getSimple().toLowerCase())) {
                        SqlNode cteQuery = ctes.get(id.getSimple().toLowerCase());
                        return recreateAsCall(asCall, cteQuery, asCall.operand(1));
                    }
                }
                return super.visit(call);
            }
        });
    }

    private static SqlNode recreateAsCall(SqlBasicCall original, SqlNode rewrittenInner, SqlNode alias) {
        if (original.operandCount() == 2) {
            return SqlStdOperatorTable.AS.createCall(original.getParserPosition(), rewrittenInner, alias);
        }
        List<SqlNode> operands = new java.util.ArrayList<>(original.operandCount());
        operands.add(rewrittenInner);
        operands.add(alias);
        for (int i = 2; i < original.operandCount(); i++) {
            operands.add(original.operand(i));
        }
        return original.getOperator().createCall(original.getParserPosition(), operands);
    }

    static Set<String> cteNames(SqlWith with) {
        java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>();
        for (SqlNode item : with.withList) {
            if (item instanceof SqlWithItem withItem) {
                names.add(withItem.name.getSimple().toLowerCase());
            }
        }
        return Set.copyOf(names);
    }

    static SqlSelect firstCteSelect(SqlWith with) {
        for (SqlNode item : with.withList) {
            if (item instanceof SqlWithItem withItem && withItem.query instanceof SqlSelect select) {
                return select;
            }
        }
        throw new IllegalArgumentException("WITH clause must contain at least one SELECT CTE");
    }

    static String firstCteName(SqlWith with) {
        for (SqlNode item : with.withList) {
            if (item instanceof SqlWithItem withItem) {
                return withItem.name.getSimple().toLowerCase();
            }
        }
        throw new IllegalArgumentException("WITH clause must contain at least one CTE");
    }

    static List<String> coordinatorDimensionTables(SqlSelect outer, Set<String> cteNames, ClusterCatalog catalog) {
        List<String> tables = new java.util.ArrayList<>();
        for (String tableName : CalciteQueryPlanner.collectTableNames(outer.getFrom())) {
            if (!cteNames.contains(tableName.toLowerCase()) && catalog.hasTable(tableName)) {
                tables.add(tableName.toLowerCase());
            }
        }
        return List.copyOf(tables);
    }
}
