package io.duckcluster.common.model;

public record TableColumn(String name, String type) {
    public TableColumn {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("column name is required");
        }
        if (type == null || type.isBlank()) {
            type = "UNKNOWN";
        }
    }
}
