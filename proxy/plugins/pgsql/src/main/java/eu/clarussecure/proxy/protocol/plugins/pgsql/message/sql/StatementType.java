package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.regex.Pattern;

public enum StatementType {
    START_TRANSACTION("BEGIN"),
    COMMIT("COMMIT"),
    ROLLBACK("ROLLBACK"),
    CREATE_TABLE("CREATE TABLE"),
    ADD_GEOMETRY_COLUMN("SELECT\\s*AddGeometryColumn"),
    INSERT("INSERT"),
    SELECT("SELECT");

    private final Pattern pattern;

    private StatementType(String keyword) {
        pattern = Pattern.compile("^\\s*" + keyword + "[\\s\r\n]*.*", Pattern.DOTALL);
    }

    public Pattern getPattern() {
        return pattern;
    }
}
