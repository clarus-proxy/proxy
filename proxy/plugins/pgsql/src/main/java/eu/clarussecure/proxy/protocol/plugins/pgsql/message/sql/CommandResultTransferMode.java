package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCommandResultMessage;

public class CommandResultTransferMode {
    private PgsqlCommandResultMessage.Details<?> newDetails;
    private TransferMode transferMode;

    public CommandResultTransferMode(PgsqlCommandResultMessage.Details<?> newDetails, TransferMode transferMode) {
        this.newDetails = newDetails;
        this.transferMode = transferMode;
    }

    public PgsqlCommandResultMessage.Details<?> getNewDetails() {
        return newDetails;
    }

    public void setNewDetails(PgsqlCommandResultMessage.Details<?> newDetails) {
        this.newDetails = newDetails;
    }

    public TransferMode getTransferMode() {
        return transferMode;
    }

    public void setTransferMode(TransferMode transferMode) {
        this.transferMode = transferMode;
    }

}
