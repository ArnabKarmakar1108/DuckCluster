package io.duckcluster.common.planner;

import io.duckcluster.common.model.OrderByClause;
import io.duckcluster.common.model.TopKSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;

import java.util.ArrayList;
import java.util.List;

public final class TopKExtractor {
    private TopKExtractor() {}

    public static TopKSpec extract(SqlNode parsed) {
        if (!(parsed instanceof SqlOrderBy orderBy)) {
            return TopKSpec.none();
        }

        List<OrderByClause> clauses = new ArrayList<>();
        if (orderBy.orderList != null) {
            for (SqlNode item : orderBy.orderList) {
                clauses.add(parseOrderByItem(item));
            }
        }
        return new TopKSpec(clauses, parseLimit(orderBy.fetch));
    }

    private static OrderByClause parseOrderByItem(SqlNode node) {
        if (node instanceof SqlBasicCall call && call.getKind() == SqlKind.DESCENDING) {
            return new OrderByClause(columnName(call.operand(0)), true);
        }
        if (node instanceof SqlIdentifier identifier) {
            return new OrderByClause(columnName(identifier), false);
        }
        if (node instanceof SqlBasicCall call) {
            return new OrderByClause(columnName(call.operand(0)), false);
        }
        throw new IllegalArgumentException("Unsupported ORDER BY expression: " + node);
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
