package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlCommandCompleteMessage implements PgsqlQueryResponseMessage<CString> {

    public static final byte TYPE = (byte) 'C';
    public static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;

    private CString tag;

    public PgsqlCommandCompleteMessage(CString tag) {
        this.tag = tag;
    }

    public CString getTag() {
        return tag;
    }

    public void setTag(CString tag) {
        this.tag = tag;
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
        builder.append("tag=").append(tag);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public CString getDetails() {
        return getTag();
    }
}
