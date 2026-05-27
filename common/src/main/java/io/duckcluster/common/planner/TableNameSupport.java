package io.duckcluster.common.planner;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

/** Resolves base table names from FROM-clause AST nodes (unwraps aliases). */
final class TableNameSupport {
    private TableNameSupport() {}

    static String baseTableName(SqlNode node) {
        if (node == null) {
            return null;
        }
        if (node instanceof SqlBasicCall call && call.getKind() == SqlKind.AS) {
            return baseTableName(call.operand(0));
        }
        if (node instanceof SqlIdentifier identifier) {
            if (identifier.names.size() == 1) {
                return identifier.getSimple();
            }
            if (identifier.names.size() == 2) {
                return identifier.names.get(1);
            }
        }
        return null;
    }
}
