package io.duckcluster.common.merger;

import io.duckcluster.proto.v1.SqlValueType;

import java.sql.Types;

/** Maps JDBC column types to merge-time SQL type names and proto enums. */
public final class SqlJdbcTypes {
    private SqlJdbcTypes() {}

    public static String toMergeSqlType(int jdbcType) {
        return switch (jdbcType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT -> "BIGINT";
            case Types.FLOAT, Types.REAL, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL -> "DOUBLE";
            case Types.BOOLEAN, Types.BIT -> "BOOLEAN";
            default -> "VARCHAR";
        };
    }

    public static SqlValueType toProtoType(int jdbcType) {
        return switch (jdbcType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT -> SqlValueType.SQL_BIGINT;
            case Types.FLOAT, Types.REAL, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL -> SqlValueType.SQL_DOUBLE;
            case Types.BOOLEAN, Types.BIT -> SqlValueType.SQL_BOOLEAN;
            default -> SqlValueType.SQL_VARCHAR;
        };
    }

    public static String fromProtoType(SqlValueType type) {
        if (type == null || type == SqlValueType.UNRECOGNIZED || type == SqlValueType.SQL_VALUE_TYPE_UNSPECIFIED) {
            return "VARCHAR";
        }
        return switch (type) {
            case SQL_BIGINT -> "BIGINT";
            case SQL_DOUBLE -> "DOUBLE";
            case SQL_BOOLEAN -> "BOOLEAN";
            default -> "VARCHAR";
        };
    }
}
