package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import io.netty.util.internal.StringUtil;

public class PgsqlReadyForQueryMessage extends PgsqlDetailedQueryResponseMessage<Byte> {

    public static final byte TYPE = (byte) 'Z';

    private byte transactionStatus;

    public PgsqlReadyForQueryMessage(byte transactionStatus) {
        this.transactionStatus = transactionStatus;
    }

    public String getTransactionStatusAsString() {
        switch ((char) transactionStatus) {
        case 'T':
            return "In a transaction";
        case 'I':
            return "Idle-not in transaction";
        case 'E':
            return "Failed transaction block";
        default:
            throw new IllegalArgumentException(
                    String.format("Invalid transaction status indicator '%c'", (char) transactionStatus));
        }
    }

    public byte getTransactionStatus() {
        return transactionStatus;
    }

    public void setTransactionStatus(byte trxStatus) {
        this.transactionStatus = trxStatus;
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("transactionStatus=").append(transactionStatus).append(':')
                .append(getTransactionStatusAsString());
        builder.append("]");
        return builder.toString();
    }

    @Override
    public Byte getDetails() {
        return getTransactionStatus();
    }
}
