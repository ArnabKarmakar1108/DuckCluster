package io.duckcluster.common.planner;

import io.duckcluster.common.model.ClusterCatalog;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Rejects SQL shapes that fragments would evaluate per shard with no coordinator-side correction, so the
 * merged answer would be silently wrong. Applies to the shard-sliced query body and its FROM-clause derived
 * tables; predicate subqueries are exempt because they scan all shards.
 */
final class DistributedQueryGuard {
    private DistributedQueryGuard() {}

    static void validate(SqlNode parsed, SqlSelect select, ClusterCatalog catalog) {
        if (parsed instanceof SqlOrderBy orderBy && orderBy.offset != null && spansMultipleShards(select, catalog)) {
            throw unsupported("OFFSET", "fetch the first OFFSET + LIMIT rows and skip on the client");
        }
        validate(select, catalog);
    }

    static void validate(SqlSelect select, ClusterCatalog catalog) {
        if (!spansMultipleShards(select, catalog)) {
            return;
        }
        if (select.isDistinct()) {
            throw unsupported("SELECT DISTINCT", "use GROUP BY on the selected columns");
        }
        if (select.getHaving() != null) {
            throw unsupported("HAVING", null);
        }
        if (select.getOffset() != null) {
            throw unsupported("OFFSET", "fetch the first OFFSET + LIMIT rows and skip on the client");
        }
        if (containsWindow(select.getSelectList()) || containsWindow(select.getOrderList())) {
            throw unsupported("Window functions (OVER)", null);
        }
        for (SqlSelect derived : derivedSelects(select.getFrom())) {
            validate(derived, catalog);
        }
    }

    private static boolean spansMultipleShards(SqlSelect select, ClusterCatalog catalog) {
        for (String table : CalciteQueryPlanner.collectTableNames(select.getFrom())) {
            if (catalog.hasTable(table) && catalog.table(table).shardCount() > 1) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsWindow(SqlNode node) {
        if (node == null) {
            return false;
        }
        boolean[] found = {false};
        node.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(SqlCall call) {
                if (call.getKind() == SqlKind.OVER) {
                    found[0] = true;
                    return null;
                }
                return super.visit(call);
            }
        });
        return found[0];
    }

    private static List<SqlSelect> derivedSelects(SqlNode from) {
        List<SqlSelect> selects = new ArrayList<>();
        collectDerived(from, selects);
        return selects;
    }

    private static void collectDerived(SqlNode node, List<SqlSelect> selects) {
        if (node instanceof SqlJoin join) {
            collectDerived(join.getLeft(), selects);
            collectDerived(join.getRight(), selects);
        } else if (node instanceof SqlSelect select) {
            selects.add(select);
        } else if (node instanceof SqlBasicCall call && call.getKind() == SqlKind.AS) {
            collectDerived(call.operand(0), selects);
        }
    }

    private static IllegalArgumentException unsupported(String feature, String hint) {
        String message = feature + " is not supported on multi-shard tables: each shard would apply it to its"
                + " own rows and the merged result would be wrong";
        return new IllegalArgumentException(hint == null ? message : message + " (" + hint + ")");
    }
}
