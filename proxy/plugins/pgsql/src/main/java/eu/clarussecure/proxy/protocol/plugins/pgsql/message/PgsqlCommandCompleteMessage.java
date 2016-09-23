package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlCommandCompleteMessage implements PgsqlCommandResultMessage<CString> {

    public static final byte TYPE = (byte) 'C';
    public static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;

    protected CString tag;

    private final Details<CString> details = new Details<CString>() {

        @Override
        public CString get() {
            return getTag();
        }
    };

    public PgsqlCommandCompleteMessage(CString tag) {
        this.tag = tag;
    }

    public PgsqlCommandCompleteMessage(Details<CString> details) {
        Objects.requireNonNull(details, "details must not be null");
        this.tag = Objects.requireNonNull(details.get(), "details content must not be null");
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
    public boolean isSuccess() {
        return true;
    }

    @Override
    public Details<CString> getDetails() {
        return details;
    }

    @Override
    public void setDetails(Details<CString> details) {
        setTag(details.get());
    }

}
