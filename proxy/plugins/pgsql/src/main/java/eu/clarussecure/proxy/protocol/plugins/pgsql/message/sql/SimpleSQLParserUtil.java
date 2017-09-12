package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import eu.clarussecure.proxy.spi.CString;

public class SimpleSQLParserUtil {
    private static final Pattern INTEGER_PATTERN = Pattern.compile("\\d+");
    private static final Pattern WORD_PATTERN = Pattern.compile("[\\w_]+");

    private static class MutableInt {
        private int value;

        public MutableInt(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void increment() {
            value++;
        }
    }

    public static SQLCommandType parse(CString sql) {
        SQLCommandType[] types = SQLCommandType.values();
        MutableInt index = new MutableInt(0);
        int start = 0;
        int end = 0;
        while (types.length > 1 || (types.length == 1 && index.getValue() < getTokens(types[0]).length)) {
            start = nextUncommentedLinePosition(sql, end);
            start = nextTokenStartPosition(sql, start);
            end = nextTokenEndPosition(sql, start);
            if (end > start) {
                CString nextStmtToken = sql.subSequence(start, end);
                SQLCommandType[] res = Arrays.stream(types).filter(type -> {
                    while (true) {
                        String token = getToken(type, index.getValue());
                        if (token != null) {
                            if (token.length() == 1 && token.charAt(0) == '*') {
                                return WORD_PATTERN.matcher(nextStmtToken).matches();
                            } else if (token.equals("\\d*")) {
                                return INTEGER_PATTERN.matcher(nextStmtToken).matches();
                            } else if (token.length() > 2 && token.charAt(0) == '['
                                    && token.charAt(token.length() - 1) == ']') {
                                token = token.substring(1, token.length() - 1);
                                if (!nextStmtToken.equalsIgnoreCase(token)) {
                                    index.increment();
                                    continue;
                                }
                                return true;
                            }
                        }
                        return nextStmtToken.equalsIgnoreCase(token);
                    }
                }).toArray(SQLCommandType[]::new);
                if (res.length == 0) {
                    res = Arrays.stream(types).filter(type -> getToken(type, index.getValue()) == null)
                            .toArray(SQLCommandType[]::new);
                }
                types = res;
                index.increment();
            } else {
                types = new SQLCommandType[0];
            }
        }
        if (types.length == 1) {
            return types[0];
        }
        return null;
    }

    private static int nextUncommentedLinePosition(CString statement, int offset) {
        while (statement.startsWith("--", offset)) {
            offset++;
            boolean eol = false;
            while (!eol && (++offset) < statement.length()) {
                char c = statement.charAt(offset);
                while (c == '\r' || c == '\n' || c == '\f' || Character.getType(c) == Character.LINE_SEPARATOR) {
                    eol = true;
                    c = (++offset) < statement.length() ? statement.charAt(offset) : 0;
                }
            }
        }
        return offset;
    }

    private static int nextTokenStartPosition(CString statement, int offset) {
        while (offset < statement.length()) {
            char c = statement.charAt(offset);
            if (!Character.isWhitespace(c)) {
                break;
            }
            offset++;
        }
        return offset;
    }

    private static int nextTokenEndPosition(CString statement, int offset) {
        while (offset < statement.length()) {
            char c = statement.charAt(offset);
            if (Character.isWhitespace(c) || c == ';' || c == '(') {
                break;
            }
            offset++;
        }
        return offset;
    }

    private static Map<SQLCommandType, String[]> typeTokens = new HashMap<>();

    private static String getToken(SQLCommandType type, int index) {
        String[] tokens = getTokens(type);
        return tokens.length > index ? tokens[index] : null;
    }

    private static String[] getTokens(SQLCommandType type) {
        String[] tokens = typeTokens.get(type);
        if (tokens == null) {
            String pattern = type.getPattern();
            tokens = pattern.split(" ");
            typeTokens.put(type, tokens);
        }
        return tokens;
    }
}
