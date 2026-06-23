package io.duckcluster.benchmark;

public enum Engine {
    DUCKCLUSTER("duckcluster"),
    DUCKDB_SINGLE("duckdb-single");

    private final String id;

    Engine(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public static Engine fromId(String id) {
        for (Engine engine : values()) {
            if (engine.id.equalsIgnoreCase(id)) {
                return engine;
            }
        }
        throw new IllegalArgumentException("Unknown engine: " + id);
    }
}
