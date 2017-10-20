package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import io.netty.util.internal.StringUtil;

public class PgsqlSSLRequestMessage implements PgsqlSessionInitializationRequestMessage {

    public static final byte TYPE = (byte) -1;
    public static final int HEADER_SIZE = Integer.BYTES;
    public static final int LENGTH = HEADER_SIZE + Integer.BYTES;
    public static final int CODE = 0x4D2162F;

    private int code;

    public PgsqlSSLRequestMessage(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
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
