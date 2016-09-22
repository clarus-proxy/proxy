package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import eu.clarussecure.proxy.spi.CString;

public class StatementTransferMode {
    private CString newStatements;
    private TransferMode transferMode;
    private CString response;

    public StatementTransferMode(CString newStatements, TransferMode transferMode) {
        this(newStatements, transferMode, null);
    }

    public StatementTransferMode(CString newStatements, TransferMode transferMode, CString response) {
        this.newStatements = newStatements;
        this.transferMode = transferMode;
        this.response = response;
    }

    public CString getNewStatements() {
        return newStatements;
    }

    public void setNewStatements(CString newStatements) {
        this.newStatements = newStatements;
    }

    public TransferMode getTransferMode() {
        return transferMode;
    }

    public void setTransferMode(TransferMode transferMode) {
        this.transferMode = transferMode;
    }

    public CString getResponse() {
        return response;
    }

    public void setResponse(CString response) {
        this.response = response;
    }

}
