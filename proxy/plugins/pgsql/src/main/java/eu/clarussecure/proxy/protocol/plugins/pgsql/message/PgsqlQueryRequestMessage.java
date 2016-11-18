package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import io.netty.util.internal.StringUtil;

public abstract class PgsqlQueryRequestMessage implements PgsqlMessage {

    public static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;

    @Override
    public int getHeaderSize() {
        return HEADER_SIZE;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this);
    }
}
