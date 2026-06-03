package io.duckcluster.common.planner;

import io.duckcluster.common.model.PlannedQuery;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpchCorrelatedExistsTest {

    private final CalciteQueryPlanner planner = new CalciteQueryPlanner();
    private final io.duckcluster.common.model.ClusterCatalog tpchCatalog = TpchCatalog.create();

    @Test
    void plansQ04WithShardLocalExistsSubquery() throws Exception {
        PlannedQuery planned = planner.plan(loadQuery("Q04.sql"), tpchCatalog);

        assertTrue(planned.correlatedCoPartitionTables().contains("lineitem"));

        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("orders_shard0"), fragmentSql);
        assertTrue(fragmentSql.contains("lineitem_shard0"), fragmentSql);
        assertTrue(fragmentSql.contains("EXISTS"), fragmentSql);
        assertTrue(!fragmentSql.contains("UNION ALL"), fragmentSql);
    }

    @Test
    void detectsQ21ExistsAsCorrelatedByAlias() throws Exception {
        SqlSelect select = selectFrom(loadQuery("Q21.sql"));
        SqlCall exists = findPredicateSubquery(select.getWhere(), "EXISTS");
        assertTrue(SubqueryAnalyzer.isCorrelated(
                SubqueryAnalyzer.operandSubquery(exists), FromScope.of(select.getFrom())));
    }

    @Test
    void plansQ21ExistsUsesShardLocalLineitem() throws Exception {
        PlannedQuery planned = planner.plan(loadQuery("Q21.sql"), tpchCatalog);
        String fragmentSql = planned.fragments().get(0).sql();
        assertTrue(fragmentSql.contains("EXISTS"), fragmentSql);
        assertTrue(fragmentSql.contains("\"lineitem_shard0\".\"lineitem\" AS \"l2\""), fragmentSql);
        assertTrue(fragmentSql.contains("\"lineitem_shard0\".\"lineitem\" AS \"l3\""), fragmentSql);
    }

    private static SqlSelect selectFrom(String sql) {
        var parsed = new CalciteQueryPlanner().parse(sql);
        if (parsed instanceof SqlSelect select) {
            return select;
        }
        return (SqlSelect) ((org.apache.calcite.sql.SqlOrderBy) parsed).query;
    }

    private static SqlCall findPredicateSubquery(org.apache.calcite.sql.SqlNode node, String opName) {
        if (node instanceof SqlCall call) {
            if (opName.equals(call.getOperator().getName()) && SubqueryAnalyzer.isPredicateSubquery(call)) {
                return call;
            }
            for (org.apache.calcite.sql.SqlNode operand : call.getOperandList()) {
                SqlCall nested = findPredicateSubquery(operand, opName);
                if (nested != null) {
                    return nested;
                }
            }
        }
        return null;
    }

    private static String loadQuery(String name) throws Exception {
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
