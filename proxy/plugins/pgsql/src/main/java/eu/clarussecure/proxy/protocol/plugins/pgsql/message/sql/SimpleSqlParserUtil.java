package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import eu.clarussecure.proxy.spi.CString;

public class SimpleSqlParserUtil {

    public static StatementType parse(CString statement) {
        StatementType[] types = StatementType.values();
        int i = 0;
        int start = 0;
        int end = 0;
        while (types.length > 1) {
            start = nextTokenStartPosition(statement, end);
            end = nextTokenEndPosition(statement, start);
            CString nextStmtToken = statement.subSequence(start, end);
            final int index = i ++;
            StatementType[] res = Arrays.stream(types).filter(type -> nextStmtToken.equals(getToken(type, index))).toArray(StatementType[]::new);
            if (res.length == 0) {
                res = Arrays.stream(types).filter(type -> getToken(type, index) == null).toArray(StatementType[]::new);
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
            if (c != ' ' && c != '\t' && c != '\n' && c != 0x0B && c != '\f' && c != '\r') {
                break;
            }
            i ++;
        }
        return i;
    }

    private static int nextTokenEndPosition(CString statement, int offset) {
        int i = offset;
        while (i < statement.length()) {
            char c = statement.charAt(i);
            if (!Character.isAlphabetic(c) && !Character.isDigit(c)) {
                break;
            }
            i ++;
        }
        return i;
    }

    private static Map<StatementType, String[]> typeTokens = new HashMap<>();

    private static String getToken(StatementType type, int index) {
        String[] tokens = getTokens(type);
        return tokens.length > index ? tokens[index] : null;
    }

    private static String[] getTokens(StatementType type) {
        String[] tokens = typeTokens.get(type);
        if (tokens == null) {
            tokens = type.getPattern().split(" ");
            typeTokens.put(type, tokens);
        }
        return tokens;
    }
}
