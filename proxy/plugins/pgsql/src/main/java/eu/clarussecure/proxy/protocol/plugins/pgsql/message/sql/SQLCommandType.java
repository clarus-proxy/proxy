package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

public enum SQLCommandType {
    START_TRANSACTION("BEGIN"), COMMIT("COMMIT"), ROLLBACK("ROLLBACK"), CREATE_TABLE(
            "CREATE TABLE"), ADD_GEOMETRY_COLUMN("SELECT AddGeometryColumn"), CLARUS_METADATA(
                    "SELECT " + PgsqlEventProcessor.FUNCTION_METADATA), CLARUS_PROTECTED(
                            "SELECT " + PgsqlEventProcessor.FUNCTION_PROTECTED), INSERT(
                                    "INSERT"), UPDATE("UPDATE"), DELETE("DELETE"), SELECT("SELECT");

    private final String pattern;

    private SQLCommandType(String pattern) {
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }
}
