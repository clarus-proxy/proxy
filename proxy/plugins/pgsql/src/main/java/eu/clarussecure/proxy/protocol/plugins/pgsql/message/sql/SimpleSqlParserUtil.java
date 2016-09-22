package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.clarussecure.proxy.spi.CString;

public class SimpleSqlParserUtil {

    public static StatementType parse(CString statement) {
        for (StatementType type : StatementType.values()) {
            Pattern pattern = type.getPattern();
            Matcher m = pattern.matcher(statement);
            if (m.matches()) {
                return type;
            }
        }
        return null;
    }
}
