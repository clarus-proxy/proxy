package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.clarussecure.proxy.spi.CString;

public class QueriesTransferMode<Q extends Query, R> {
    private List<Query> newQueries;
    private TransferMode transferMode;
    private R response;
    private Map<Byte, CString> errorDetails;

    public QueriesTransferMode(Q newQuery, TransferMode transferMode) {
        this(Collections.singletonList(newQuery), transferMode);
    }

    public QueriesTransferMode(List<Query> newQueries, TransferMode transferMode) {
        this(newQueries, transferMode, null, null);
    }

    public QueriesTransferMode(List<Query> newQueries, TransferMode transferMode, R response) {
        this(newQueries, transferMode, response, null);
    }

    public QueriesTransferMode(List<Query> newQueries, TransferMode transferMode, Map<Byte, CString> errorDetails) {
        this(newQueries, transferMode, null, errorDetails);
    }

    public QueriesTransferMode(List<Query> newQueries, TransferMode transferMode, R response, Map<Byte, CString> errorDetails) {
        this.newQueries = newQueries;
        this.transferMode = transferMode;
        this.response = response;
        this.errorDetails = errorDetails;
    }

    public List<Query> getNewQueries() {
        return newQueries;
    }

    public void setNewQueries(List<Query> newQueries) {
        this.newQueries = newQueries;
    }

    @SuppressWarnings("unchecked")
    public Q getLastNewQuery() {
        return newQueries.size() > 0 ? (Q) newQueries.get(newQueries.size() - 1) : null;
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
