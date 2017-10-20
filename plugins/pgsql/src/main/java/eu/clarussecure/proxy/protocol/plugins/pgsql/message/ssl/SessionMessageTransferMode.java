package eu.clarussecure.proxy.protocol.plugins.pgsql.message.ssl;

import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.TransferMode;
import eu.clarussecure.proxy.spi.CString;

public class SessionMessageTransferMode<D, R> {
    private D newDetails;
    private TransferMode transferMode;
    private R response;
    private Map<Byte, CString> errorDetails;

    public SessionMessageTransferMode(D newDetails, TransferMode transferMode) {
        this(newDetails, transferMode, null, null);
    }

    public SessionMessageTransferMode(D newDetails, TransferMode transferMode, R response) {
        this(newDetails, transferMode, response, null);
    }

    public SessionMessageTransferMode(D newDetails, TransferMode transferMode, Map<Byte, CString> errorDetails) {
        this(newDetails, transferMode, null, errorDetails);
    }

    public SessionMessageTransferMode(D newDetails, TransferMode transferMode, R response,
            Map<Byte, CString> errorDetails) {
        this.newDetails = newDetails;
        this.transferMode = transferMode;
        this.response = response;
        this.errorDetails = errorDetails;
    }

    public D getNewDetails() {
        return newDetails;
    }

    public void setNewDetails(D newDetails) {
        this.newDetails = newDetails;
    }

    public TransferMode getTransferMode() {
        return transferMode;
    }

    public void setTransferMode(TransferMode transferMode) {
        this.transferMode = transferMode;
    }

    public R getResponse() {
        return response;
    }

    public void setResponse(R response) {
        this.response = response;
    }

    public Map<Byte, CString> getErrorDetails() {
        return errorDetails;
    }

    public void setErrorDetails(Map<Byte, CString> error) {
        this.errorDetails = error;
    }
}
