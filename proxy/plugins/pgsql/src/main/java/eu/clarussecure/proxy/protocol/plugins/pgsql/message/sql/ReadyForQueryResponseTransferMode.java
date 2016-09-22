package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

public class ReadyForQueryResponseTransferMode {
    private Byte newTransactionStatus;
    private TransferMode transferMode;

    public ReadyForQueryResponseTransferMode(Byte newTransactionStatus, TransferMode transferMode) {
        this.newTransactionStatus = newTransactionStatus;
        this.transferMode = transferMode;
    }

    public Byte getNewTransactionStatus() {
        return newTransactionStatus;
    }

    public void setNewTransactionStatus(Byte newTransactionStatus) {
        this.newTransactionStatus = newTransactionStatus;
    }

    public TransferMode getTransferMode() {
        return transferMode;
    }

    public void setTransferMode(TransferMode transferMode) {
        this.transferMode = transferMode;
    }

}
