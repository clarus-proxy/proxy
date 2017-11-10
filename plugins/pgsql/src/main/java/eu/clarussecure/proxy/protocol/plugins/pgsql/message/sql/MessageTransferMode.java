package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.clarussecure.proxy.spi.CString;

public class MessageTransferMode<C, R> {
    private TransferMode transferMode;
    private List<C> newContents;
    private boolean directed;
    private R response;
    private Map<Byte, CString> errorDetails;

    public MessageTransferMode(TransferMode transferMode, C newContent) {
        this(transferMode, newContent, null, null);
    }

    public MessageTransferMode(TransferMode transferMode, C newContent, R response) {
        this(transferMode, newContent, response, null);
    }

    public MessageTransferMode(TransferMode transferMode, C newContent, R response, Map<Byte, CString> errorDetails) {
        this(transferMode, Collections.singletonList(newContent), false, response, errorDetails);
    }

    public MessageTransferMode(TransferMode transferMode, List<C> newDirectedContents) {
        this(transferMode, newDirectedContents, null, null);
    }

    public MessageTransferMode(TransferMode transferMode, List<C> newDirectedContents, R response) {
        this(transferMode, newDirectedContents, response, null);
    }

    public MessageTransferMode(TransferMode transferMode, List<C> newDirectedContents, R response,
            Map<Byte, CString> errorDetails) {
        this(transferMode, newDirectedContents, true, response, null);
    }

    public MessageTransferMode(TransferMode transferMode, List<C> newContents, boolean directed, R response,
            Map<Byte, CString> errorDetails) {
        this.transferMode = transferMode;
        this.newContents = newContents;
        this.directed = directed;
        this.response = response;
        this.errorDetails = errorDetails;
    }

    public TransferMode getTransferMode() {
        return transferMode;
    }

    public void setTransferMode(TransferMode transferMode) {
        this.transferMode = transferMode;
    }

    public C getNewContent() {
        return newContents.get(0);
    }

    public void setNewContent(C newContent) {
        setNewDirectedContents(Collections.singletonList(newContent));
    }

    public List<C> getNewDirectedContents() {
        return newContents;
    }

    public void setNewDirectedContents(List<C> newDirectedContents) {
        this.newContents = newDirectedContents;
    }

    public boolean isDirected() {
        return directed;
    }

    public void setDirected(boolean directed) {
        this.directed = directed;
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
