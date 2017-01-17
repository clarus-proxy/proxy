package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

public class MessageTransferMode<C> {
    private C newContent;
    private TransferMode transferMode;

    public MessageTransferMode(C newContent, TransferMode transferMode) {
        this.newContent = newContent;
        this.transferMode = transferMode;
    }

    public C getNewContent() {
        return newContent;
    }

    public void setNewContent(C newContent) {
        this.newContent = newContent;
    }

    public TransferMode getTransferMode() {
        return transferMode;
    }

    public void setTransferMode(TransferMode transferMode) {
        this.transferMode = transferMode;
    }

}
