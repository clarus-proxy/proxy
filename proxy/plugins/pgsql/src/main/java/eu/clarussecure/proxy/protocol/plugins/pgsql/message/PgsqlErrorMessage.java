package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlErrorMessage implements PgsqlCommandResultMessage {

    public static final byte TYPE = (byte) 'E';
    public static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;
    public static final String ERROR_TAG = "ERROR:\n";

    private Map<Byte, CString> fields;

    public PgsqlErrorMessage(Map<Byte, CString> fields) {
        this.fields = Objects.requireNonNull(fields, "fields must not be null");
    }

    public PgsqlErrorMessage(CString fieldsAsString) {
        this(parseFields(fieldsAsString));
    }

    public Map<Byte, CString> getFields() {
        return fields;
    }

    public CString getFieldsAsString() {
        StringBuilder builder = new StringBuilder(ERROR_TAG);
        int i = 0;
        for (Map.Entry<Byte, CString> entry : fields.entrySet()) {
            builder.append("'").append((char) entry.getKey().byteValue()).append("':").append(entry.getValue());
            if (++i < fields.size()) {
                builder.append("\n");
            }
        }
        return CString.valueOf(builder.toString());
    }

    public static boolean isErrorFields(CString str) {
        return (str.length() >= ERROR_TAG.length()) && str.subSequence(0, ERROR_TAG.length()).equals(ERROR_TAG);
    }

    private static Map<Byte, CString> parseFields(CString str) {
        if (!isErrorFields(str)) {
            throw new IllegalArgumentException("str must start with " + ERROR_TAG);
        }
        String[] tokens = str.toString().split("\n");
        Map<Byte, CString> fields = new LinkedHashMap<>();
        for (int i = 1; i < tokens.length; i ++) {
            String token = tokens[i];
            byte code = (byte) token.charAt(0);
            CString value = CString.valueOf(token.substring(2, token.length()));
            fields.put(code, value);
        }
        return fields;
    }

    public void setFields(Map<Byte, CString> fields) {
        this.fields = fields;
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public int getHeaderSize() {
        return HEADER_SIZE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("fields=").append(getFieldsAsString());
        builder.append("]");
        return builder.toString();
    }


    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public CString getDetails() {
        return getFieldsAsString();
    }

    @Override
    public void setDetails(CString details) {
        setFields(parseFields(details));
    }
}
