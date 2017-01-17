package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlPasswordMessage implements PgsqlAuthenticationMessage {

    public static final byte TYPE = (byte) 'p';
    public static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;
    private CString password;

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public int getHeaderSize() {
        return HEADER_SIZE;
    }

    public PgsqlPasswordMessage(CString password) {
        this.password = password;
    }

    public CString getPassword() {
        return password;
    }

    public void setPassword(CString password) {
        this.password = password;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("password=").append(password);
        builder.append("]");
        return builder.toString();
    }
}
