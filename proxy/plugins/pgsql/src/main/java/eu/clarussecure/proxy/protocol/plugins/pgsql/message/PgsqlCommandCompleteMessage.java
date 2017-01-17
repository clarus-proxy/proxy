package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlCommandCompleteMessage extends PgsqlDetailedQueryResponseMessage<CString> {

    public static final byte TYPE = (byte) 'C';

    private CString tag;

    public PgsqlCommandCompleteMessage(CString tag) {
        this.tag = Objects.requireNonNull(tag, "tag must not be null");
    }

    public CString getTag() {
        return tag;
    }

    public void setTag(CString tag) {
        this.tag = Objects.requireNonNull(tag, "tag must not be null");
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("tag=").append(tag);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public CString getDetails() {
        return getTag();
    }
}
