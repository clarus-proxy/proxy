package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import io.netty.util.internal.StringUtil;

public class PgsqlCancelRequestMessage implements PgsqlSessionInitializationRequestMessage {

    public static final byte TYPE = (byte) -3;
    public static final int HEADER_SIZE = Integer.BYTES;
    public static final int LENGTH = HEADER_SIZE + 3 * Integer.BYTES;
    public static final int CODE = 0x4D2162E;

    private int code;
    private int processId;
    private int secretKey;

    public PgsqlCancelRequestMessage(int code, int processId, int secretKey) {
        this.code = code;
        this.processId = processId;
        this.secretKey = secretKey;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getProcessId() {
        return processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    public int getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(int secretKey) {
        this.secretKey = secretKey;
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
        builder.append(", process ID=").append(processId);
        builder.append(", secret key=").append(secretKey);
        builder.append("]");
        return builder.toString();
    }

}
