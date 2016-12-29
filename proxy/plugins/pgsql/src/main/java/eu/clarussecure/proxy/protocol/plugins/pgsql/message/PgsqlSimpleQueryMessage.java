package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlSimpleQueryMessage extends PgsqlQueryMessage {

    public static final byte TYPE = (byte) 'Q';

    public PgsqlSimpleQueryMessage(CString query) {
        super(query);
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("query=").append(query);
        builder.append("]");
        return builder.toString();
    }
}
