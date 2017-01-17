package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import eu.clarussecure.proxy.spi.CString;

public class SimpleSQLParserUtil {

    public static SQLCommandType parse(CString sql) {
        SQLCommandType[] types = SQLCommandType.values();
        int i = 0;
        int start = 0;
        int end = 0;
        while (types.length > 1) {
            start = nextTokenStartPosition(sql, end);
            end = nextTokenEndPosition(sql, start);
            CString nextStmtToken = sql.subSequence(start, end);
            final int index = i++;
            SQLCommandType[] res = Arrays.stream(types)
                    .filter(type -> nextStmtToken.equalsIgnoreCase(getToken(type, index)))
                    .toArray(SQLCommandType[]::new);
            if (res.length == 0) {
                res = Arrays.stream(types).filter(type -> getToken(type, index) == null).toArray(SQLCommandType[]::new);
            }
            types = res;
        }
        if (types.length == 1) {
            return types[0];
        }
        return null;
    }

    private static int nextTokenStartPosition(CString statement, int offset) {
        int i = offset;
        while (i < statement.length()) {
            char c = statement.charAt(i);
            if (!Character.isWhitespace(c)) {
                break;
            }
            i++;
        }
        return i;
    }

    private static int nextTokenEndPosition(CString statement, int offset) {
        int i = offset;
        while (i < statement.length()) {
            char c = statement.charAt(i);
            if (Character.isWhitespace(c) || c == ';' || c == '(') {
                break;
            }
            i++;
        }
        return i;
    }

    private static Map<SQLCommandType, String[]> typeTokens = new HashMap<>();

    private static String getToken(SQLCommandType type, int index) {
        String[] tokens = getTokens(type);
        return tokens.length > index ? tokens[index] : null;
    }

    private static String[] getTokens(SQLCommandType type) {
        String[] tokens = typeTokens.get(type);
        if (tokens == null) {
            tokens = type.getPattern().split(" ");
            typeTokens.put(type, tokens);
        }
        return tokens;
    }
}
