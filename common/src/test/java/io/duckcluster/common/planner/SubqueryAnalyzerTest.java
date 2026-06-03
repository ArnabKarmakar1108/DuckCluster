package io.duckcluster.common.planner;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubqueryAnalyzerTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();

    @Test
    void detectsCorrelatedExistsInQ04() throws Exception {
        SqlSelect select = selectFrom(loadQuery("Q04.sql"));
        SqlCall exists = findPredicateSubquery(select.getWhere());
        SqlSelect subquery = SubqueryAnalyzer.operandSubquery(exists);
        assertTrue(SubqueryAnalyzer.isCorrelated(subquery, Set.of("orders")));
    }

    @Test
    void detectsCorrelatedScalarInQ02() throws Exception {
        SqlSelect select = selectFrom(loadQuery("Q02.sql"));
        SqlCall comparison = findComparisonWithSubquery(select.getWhere());
        SqlSelect subquery = SubqueryAnalyzer.operandSubquery(comparison);
        assertTrue(SubqueryAnalyzer.isCorrelated(subquery, Set.of("part", "supplier", "partsupp", "nation", "region")));
    }

    @Test
    void detectsCorrelatedExistsByAliasInQ21() throws Exception {
        SqlSelect select = selectFrom(loadQuery("Q21.sql"));
        SqlCall exists = findPredicateSubquery(select.getWhere());
        assertTrue(SubqueryAnalyzer.isCorrelated(
                SubqueryAnalyzer.operandSubquery(exists), FromScope.of(select.getFrom())));
    }

    @Test
    void detectsUncorrelatedInSubqueryInQ18() throws Exception {
        SqlSelect select = selectFrom(loadQuery("Q18.sql"));
        SqlCall inPredicate = findPredicateSubquery(select.getWhere());
        SqlSelect subquery = SubqueryAnalyzer.operandSubquery(inPredicate);
        assertFalse(SubqueryAnalyzer.isCorrelated(subquery, Set.of("customer", "orders", "lineitem")));
    }

    @Test
    void detectsUncorrelatedNotInForSupplier() {
        SqlSelect select = selectFrom(
                "SELECT 1 FROM partsupp, part WHERE ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%x%')");
        SqlCall notIn = findNotIn(select.getWhere());
        SqlSelect subquery = SubqueryAnalyzer.operandSubquery(notIn);
        assertFalse(SubqueryAnalyzer.isCorrelated(subquery, Set.of("partsupp", "part")));
    }

    private static SqlSelect selectFrom(String sql) {
        SqlNode parsed = new CalciteQueryPlanner().parse(sql);
        if (parsed instanceof SqlSelect select) {
            return select;
        }
        return (SqlSelect) ((org.apache.calcite.sql.SqlOrderBy) parsed).query;
    }

    private static SqlCall findPredicateSubquery(SqlNode node) {
        if (node instanceof SqlCall call) {
            if (SubqueryAnalyzer.isPredicateSubquery(call)) {
                return call;
            }
            for (SqlNode operand : call.getOperandList()) {
                SqlCall nested = findPredicateSubquery(operand);
                if (nested != null) {
                    return nested;
                }
            }
        }
        return null;
    }

    private static SqlCall findComparisonWithSubquery(SqlNode node) {
        if (node instanceof SqlCall call) {
            if (SubqueryAnalyzer.operandSubquery(call) != null) {
                return call;
            }
            for (SqlNode operand : call.getOperandList()) {
                SqlCall nested = findComparisonWithSubquery(operand);
                if (nested != null) {
                    return nested;
                }
            }
        }
        return null;
    }

    private static SqlCall findNotIn(SqlNode node) {
        if (node instanceof SqlCall call) {
            if (call.getKind().toString().contains("NOT_IN")) {
                return call;
            }
            for (SqlNode operand : call.getOperandList()) {
                SqlCall nested = findNotIn(operand);
                if (nested != null) {
                    return nested;
                }
            }
        }
        return null;
    }

    private String loadQuery(String name) throws Exception {
        Path path = Path.of("..", "benchmark", "src", "main", "resources", "queries", name).normalize();
        if (!Files.exists(path)) {
            path = Path.of("benchmark", "src", "main", "resources", "queries", name);
        }
        String sql = Files.readString(path);
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return sql;
    }
}
