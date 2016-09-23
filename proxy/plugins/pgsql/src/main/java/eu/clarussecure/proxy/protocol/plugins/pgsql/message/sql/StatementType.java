package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

public enum StatementType {
    START_TRANSACTION("BEGIN"),
    COMMIT("COMMIT"),
    ROLLBACK("ROLLBACK"),
    CREATE_TABLE("CREATE TABLE"),
    ADD_GEOMETRY_COLUMN("SELECT AddGeometryColumn"),
    INSERT("INSERT"),
    SELECT("SELECT");

    private final String pattern;

    private StatementType(String pattern) {
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }
}
