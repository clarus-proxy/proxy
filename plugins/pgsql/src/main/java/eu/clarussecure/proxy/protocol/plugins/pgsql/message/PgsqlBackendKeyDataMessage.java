package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import io.netty.util.internal.StringUtil;

public class PgsqlBackendKeyDataMessage extends PgsqlQueryResponseMessage {

    public static final byte TYPE = (byte) 'K';

    private int processId;
    private int secretKey;

    public PgsqlBackendKeyDataMessage(int processId, int secretKey) {
        this.processId = processId;
        this.secretKey = secretKey;
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
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("process ID=").append(processId);
        builder.append(", secret key=").append(secretKey);
        builder.append("]");
        return builder.toString();
    }
}
