package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.Map;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlErrorMessage implements PgsqlCommandResultMessage<Map<Byte, CString>> {

    public static final byte TYPE = (byte) 'E';
    public static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;

    private Map<Byte, CString> fields;

    private final Details<Map<Byte, CString>> details = new Details<Map<Byte, CString>>() {

        @Override
        public Map<Byte, CString> get() {
            return getFields();
        }
    };

    public PgsqlErrorMessage(Map<Byte, CString> fields) {
        this.fields = Objects.requireNonNull(fields, "fields must not be null");
    }

    public PgsqlErrorMessage(Details<Map<Byte, CString>> details) {
        Objects.requireNonNull(details, "details must not be null");
        this.fields = Objects.requireNonNull(details.get(), "details content must not be null");
    }

    public Map<Byte, CString> getFields() {
        return fields;
    }

    public void setFields(Map<Byte, CString> fields) {
        this.fields = Objects.requireNonNull(fields, "fields must not be null");
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
        builder.append("fields=");
        int i = 0;
        for (Map.Entry<Byte, CString> entry : fields.entrySet()) {
            builder.append("'").append((char) entry.getKey().byteValue()).append("':").append(entry.getValue());
            if (++i < fields.size()) {
                builder.append("\n");
            }
        }
        builder.append("]");
        return builder.toString();
    }


    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public Details<Map<Byte, CString>> getDetails() {
        return details;
    }

    @Override
    public void setDetails(Details<Map<Byte, CString>> details) {
        setFields(details.get());
    }
}
