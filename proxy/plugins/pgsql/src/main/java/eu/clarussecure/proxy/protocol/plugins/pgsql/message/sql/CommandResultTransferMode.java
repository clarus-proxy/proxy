package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import eu.clarussecure.proxy.spi.CString;

public class CommandResultTransferMode {
    private CString newDetails;
    private TransferMode transferMode;

    public CommandResultTransferMode(CString newDetails, TransferMode transferMode) {
        this.newDetails = newDetails;
        this.transferMode = transferMode;
    }

    public CString getNewDetails() {
        return newDetails;
    }

    public void setNewDetails(CString newDetails) {
        this.newDetails = newDetails;
    }

    public TransferMode getTransferMode() {
        return transferMode;
    }

    public void setTransferMode(TransferMode transferMode) {
        this.transferMode = transferMode;
    }

}
