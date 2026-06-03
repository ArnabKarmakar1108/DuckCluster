package io.duckcluster.common.planner;

import io.duckcluster.common.model.OrderByClause;
import io.duckcluster.common.model.TopKSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;

public final class TopKExtractor {
    private TopKExtractor() {}

    public static TopKSpec extract(SqlNode parsed) {
        return extract(parsed, null);
    }

    public static TopKSpec extract(SqlNode parsed, SqlSelect select) {
        if (!(parsed instanceof SqlOrderBy orderBy)) {
            return TopKSpec.none();
        }

        List<OrderByClause> clauses = new ArrayList<>();
        if (orderBy.orderList != null) {
            for (SqlNode item : orderBy.orderList) {
                clauses.add(parseOrderByItem(item, select));
            }
        }
        return new TopKSpec(clauses, parseLimit(orderBy.fetch));
    }

    private static OrderByClause parseOrderByItem(SqlNode node, SqlSelect select) {
        if (node instanceof SqlBasicCall call && call.getKind() == SqlKind.DESCENDING) {
            return new OrderByClause(resolveName(call.operand(0), select), true);
        }
        if (node instanceof SqlIdentifier identifier) {
            return new OrderByClause(resolveName(identifier, select), false);
        }
        if (node instanceof SqlBasicCall call) {
            return new OrderByClause(resolveName(call.operand(0), select), false);
        }
        throw new IllegalArgumentException("Unsupported ORDER BY expression: " + node);
    }

    private static String resolveName(SqlNode node, SqlSelect select) {
        String name = columnName(node);
        if (select != null && select.getSelectList() != null) {
            return TopKResolver.resolveFromSelectList(name, select.getSelectList());
        }
        return name;
    }

    private static int parseLimit(SqlNode fetch) {
        if (fetch == null) {
            return -1;
        }
        if (fetch instanceof SqlNumericLiteral literal) {
            return literal.intValue(true);
        }
        throw new IllegalArgumentException("Unsupported LIMIT expression: " + fetch);
    }

    private static String columnName(SqlNode node) {
        if (node instanceof SqlIdentifier identifier) {
            List<String> names = identifier.names;
            return names.get(names.size() - 1);
        }
        throw new IllegalArgumentException("Expected column identifier but found: " + node);
    }
}
