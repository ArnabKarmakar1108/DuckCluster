package io.duckcluster.common.planner;

import io.duckcluster.common.model.TopKSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class NestedDerivedTableDetector {
    private NestedDerivedTableDetector() {}

    record Match(SqlSelect innerSelect, List<String> derivedColumnNames, TopKSpec outerTopK) {}

    static Optional<Match> detect(SqlSelect outer, SqlNode parsed) {
        SqlNode from = outer.getFrom();
        if (!(from instanceof SqlBasicCall call) || call.getKind() != SqlKind.AS) {
            return Optional.empty();
        }
        SqlNode innerNode = call.operand(0);
        if (!(innerNode instanceof SqlSelect inner)) {
            return Optional.empty();
        }
        if (inner.getGroup() == null || inner.getGroup().isEmpty()) {
            return Optional.empty();
        }
        List<String> derivedColumns = extractDerivedColumnNames(call);
        TopKSpec outerTopK = TopKExtractor.extract(parsed);
        return Optional.of(new Match(inner, derivedColumns, outerTopK));
    }

    private static List<String> extractDerivedColumnNames(SqlBasicCall asCall) {
        List<String> columns = new ArrayList<>();
        for (int i = 2; i < asCall.operandCount(); i++) {
            SqlNode operand = asCall.operand(i);
            if (operand instanceof SqlIdentifier identifier) {
                columns.add(identifier.getSimple());
            }
        }
        return List.copyOf(columns);
    }
}
