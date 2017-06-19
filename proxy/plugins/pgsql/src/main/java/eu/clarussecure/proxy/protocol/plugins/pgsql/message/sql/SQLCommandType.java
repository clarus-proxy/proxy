package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

public enum SQLCommandType {
    // Transaction commands
    START_TRANSACTION("BEGIN"),
    COMMIT("COMMIT"),
    ROLLBACK("ROLLBACK"),
    // Dataset operation
    CREATE_TABLE("CREATE TABLE"),
    ALTER_TABLE("ALTER TABLE"),
    ADD_GEOMETRY_COLUMN("SELECT " + PgsqlEventProcessor.FUNCTION_ADD_GEOMETRY_COLUMN),
    DROP_TABLE("DROP TABLE"),
    // Data operation
    INSERT("INSERT"),
    UPDATE("UPDATE"),
    DELETE("DELETE"),
    SELECT("SELECT"),
    // Cursor
    DECLARE_CURSOR("DECLARE * [BINARY] [INSENSITIVE] [NO] [SCROLL] CURSOR"),
    FETCH_CURSOR(
            "FETCH [NEXT] [PRIOR] [FIRST] [LAST] [ABSOLUTE] [RELATIVE] [ALL] [FORWARD] [BACKWARD] \\d* [FROM] [IN] *"),
    CLOSE_CURSOR("CLOSE *"),
    // Other
    SET("SET"),
    // Clarus metadata function
    CLARUS_METADATA("SELECT " + PgsqlEventProcessor.FUNCTION_METADATA),
    // Clarus protected function
    CLARUS_PROTECTED("SELECT " + PgsqlEventProcessor.FUNCTION_PROTECTED);

    private final String pattern;

    private SQLCommandType(String pattern) {
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }
}
