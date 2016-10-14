package eu.clarussecure.proxy.spi;

public class StringUtilities {

    public static String toString(CharSequence cs) {
        return cs == null ? null : cs.toString();
    }

    public static String unquote(String str) {
        int length = str == null ? -1 : str.length();
        if (str == null || length == 0)
            return str;

        if (length > 1 && ((str.charAt(0) == '\"' && str.charAt(length - 1) == '\"') || (str.charAt(0) == '\'' && str.charAt(length - 1) == '\''))) {
            str = str.substring(1, length - 1);
        }

        return str;
    }

    public static String quote(String str) {
        int length = str == null ? -1 : str.length();
        if (str == null || length == 0)
            return str;

        if (str.charAt(0) != '\"' && str.charAt(length - 1) != '\"') {
            str = "\"" + str + "\"";
        }

        return str;
    }

    public static String singleQuote(String str) {
        int length = str == null ? -1 : str.length();
        if (str == null || length == 0)
            return str;

        if (str.charAt(0) != '\'' && str.charAt(length - 1) != '\'') {
            str = "\'" + str + "\'";
        }

        return str;
    }

    public static String addIrrelevantCharacters(String newStatement, CharSequence statement, String irrelevantCharacters) {
        // Append irrelevant characters at the begin of the original statement
        int index;
        for (index = 0; index < statement.length(); index ++) {
            char c = statement.charAt(index);
            if (irrelevantCharacters.indexOf(c) == -1) {
                break;
            }
        }
        if (index > 0) {
            newStatement = statement.subSequence(0, index) + newStatement;
        }
        // Append irrelevant characters at the end of the original statement
        for (index = statement.length(); index > 0; index --) {
            char c = statement.charAt(index - 1);
            if (irrelevantCharacters.indexOf(c) == -1) {
                break;
            }
        }
        if (index < statement.length()) {
            newStatement += statement.subSequence(index, statement.length());
        }
        return newStatement;
    }
}
