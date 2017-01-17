package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.Map;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlNoticeMessage extends PgsqlDetailedQueryResponseMessage<Map<Byte, CString>> {

    public static final byte TYPE = (byte) 'N';

    private Map<Byte, CString> fields;

    public PgsqlNoticeMessage(Map<Byte, CString> fields) {
        this.fields = Objects.requireNonNull(fields, "fields must not be null");
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
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("fields=");
        int i = 0;
        for (Map.Entry<Byte, CString> entry : fields.entrySet()) {
            builder.append("'").append((char) entry.getKey().byteValue()).append("':").append(entry.getValue());
            if (++i < fields.size()) {
                builder.append('\n');
            }
        }
        builder.append("]");
        return builder.toString();
    }

    @Override
    public Map<Byte, CString> getDetails() {
        return getFields();
    }
}
