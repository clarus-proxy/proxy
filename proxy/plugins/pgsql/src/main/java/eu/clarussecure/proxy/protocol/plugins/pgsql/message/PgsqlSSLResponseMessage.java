package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import io.netty.util.internal.StringUtil;

public class PgsqlSSLResponseMessage implements PgsqlSessionInitializationResponseMessage {

    public static final byte TYPE = (byte) -2;
    public static final int HEADER_SIZE = 0;
    public static final int LENGTH = HEADER_SIZE + Byte.BYTES;
    public static final byte CODE_SSL = 'S';
    public static final byte CODE_NO_SSL = 'N';

    private byte code;

    public PgsqlSSLResponseMessage(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public void setCode(byte code) {
        this.code = code;
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
        builder.append("code=").append(code);
        builder.append("]");
        return builder.toString();
    }

}
