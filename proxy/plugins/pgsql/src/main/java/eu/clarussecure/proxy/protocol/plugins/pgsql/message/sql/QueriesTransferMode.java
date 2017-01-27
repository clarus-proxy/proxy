package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.clarussecure.proxy.spi.CString;

public class QueriesTransferMode<Q extends Query, R> {
    private TransferMode transferMode;
    private Map<Integer, List<Query>> newQueries;
    private R response;
    private Map<Byte, CString> errorDetails;

    public QueriesTransferMode(TransferMode transferMode, Q newQuery) {
        this(transferMode, Collections.singletonList(newQuery));
    }

    public QueriesTransferMode(TransferMode transferMode, List<Query> newQueries) {
        this(transferMode, newQueries, null, null);
    }

    public QueriesTransferMode(TransferMode transferMode, List<Query> newQueries, R response) {
        this(transferMode, newQueries, response, null);
    }

    public QueriesTransferMode(TransferMode transferMode, List<Query> newQueries, R response,
            Map<Byte, CString> errorDetails) {
        this(transferMode, Collections.singletonMap(0, newQueries), response, errorDetails);
    }

    public QueriesTransferMode(TransferMode transferMode, Map<Integer, List<Query>> newDirectedQueries) {
        this(transferMode, newDirectedQueries, null, null);
    }

    public QueriesTransferMode(TransferMode transferMode, Map<Integer, List<Query>> newDirectedQueries, R response) {
        this(transferMode, newDirectedQueries, response, null);
    }

    public QueriesTransferMode(TransferMode transferMode, Map<Integer, List<Query>> newQueries, R response,
            Map<Byte, CString> errorDetails) {
        this.transferMode = transferMode;
        this.newQueries = newQueries;
        this.response = response;
        this.errorDetails = errorDetails;
    }

    public TransferMode getTransferMode() {
        return transferMode;
    }

    public void setTransferMode(TransferMode transferMode) {
        this.transferMode = transferMode;
    }

    public List<Query> getNewQueries() {
        return newQueries == null ? Collections.emptyList() : newQueries.get(0);
    }

    @SuppressWarnings("unchecked")
    public Q getLastNewQuery() {
        return newQueries == null || newQueries.isEmpty() || newQueries.get(0).isEmpty() ? null
                : (Q) newQueries.get(0).get(newQueries.get(0).size() - 1);
    }

    public void setNewQueries(List<Query> newQueries) {
        setNewDirectedQueries(Collections.singletonMap(0, newQueries));
    }

    public Map<Integer, List<Query>> getNewDirectedQueries() {
        return newQueries;
    }

    public void setNewDirectedQueries(Map<Integer, List<Query>> newDirectedQueries) {
        this.newQueries = newDirectedQueries;
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
