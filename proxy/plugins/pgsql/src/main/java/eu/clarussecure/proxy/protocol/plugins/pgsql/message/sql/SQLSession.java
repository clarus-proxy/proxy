package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import eu.clarussecure.dataoperations.Promise;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.Operation;
import io.netty.buffer.ByteBuf;

public class SQLSession {
    public static class ExtendedQueryStatus<Q extends ExtendedQuery> {
        private final Q query;
        private final Operation operation;
        private final boolean toProcess;
        private boolean processed;

        public ExtendedQueryStatus(Q query, Operation operation) {
            this(query, operation, false);
        }

        public ExtendedQueryStatus(Q query, Operation operation, boolean toProcess) {
            this.query = query;
            this.operation = operation;
            this.toProcess = toProcess;
            this.processed = false;
        }

        public Q getQuery() {
            return query;
        }

        public Operation getOperation() {
            return operation;
        }

        public boolean isToProcess() {
            return toProcess;
        }

        public boolean isProcessed() {
            return processed;
        }

        public void setProcessed(boolean processed) {
            this.processed = processed;
        }

        public void retain() {
            query.retain();
        }

        public boolean release() {
            return query.release();
        }
    }

    public enum QueryResponseType {
        PARSE_COMPLETE, BIND_COMPLETE, PARAMETER_DESCRIPTION, ROW_DESCRIPTION_AND_ROW_DATA, NO_DATA, COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR, CLOSE_COMPLETE, READY_FOR_QUERY
    }

    private CString databaseName;
    private byte transactionStatus = (byte) 'I';
    private Map<Byte, CString> transactionErrorDetails;
    private boolean inDatasetCreation;
    private Operation currentCommandOperation;
    private Promise promise;
    private TransferMode transferMode;
    private List<Query> bufferedQueries;
    private List<PgsqlRowDescriptionMessage.Field> rowDescription;
    private Deque<QueryResponseType> queryResponsesToIgnore;
    private Map<CString, ExtendedQueryStatus<ParseStep>> parseStepStatuses;
    private Map<CString, ExtendedQueryStatus<BindStep>> bindStepStatuses;
    private Map<CString, DescribeStep> describeSteps;

    /*
     * 
     */
    // Users to save. Use of Set to avoid duplicate value.
    private CString user;
    private ByteBuf authenticationParam;
    private int authenticationType;
    
    public CString getUser() {
        return user;
    }

    public void setUser(CString user) {
        if (this.user != null) {
            // Release internal buffer of user name
            this.user.release();
        }
        user.retain();
        this.user = user;
    }

    public ByteBuf getAuthenticationParam() {
        return authenticationParam;
    }

    public void setAuthenticationParam(ByteBuf authenticationParam) {
        if (this.authenticationParam != null) {
            // Release internal buffer.
            this.authenticationParam.release();
        }
        this.authenticationParam = authenticationParam.retainedDuplicate();
    }

    public int getAuthenticationType() {
        return authenticationType;
    }

    public void setAuthenticationType(int authenticationType) {
        this.authenticationType = authenticationType;
    }
    /*
     * 
     */

    public CString getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(CString databaseName) {
        if (this.databaseName != null) {
            // Release internal buffer of database name
            this.databaseName.release();
        }
        this.databaseName = databaseName;
    }

    public byte getTransactionStatus() {
        return transactionStatus;
    }

    public void setTransactionStatus(byte transactionStatus) {
        this.transactionStatus = transactionStatus;
    }

    public Map<Byte, CString> getRetainedTransactionErrorDetails() {
        if (transactionErrorDetails != null) {
            synchronized (transactionErrorDetails) {
                for (CString fieldValue : transactionErrorDetails.values()) {
                    fieldValue.retain();
                }
            }
        }
        return transactionErrorDetails;
    }

    public void setTransactionErrorDetails(Map<Byte, CString> errorDetails) {
        if (errorDetails != null) {
            for (CString fieldValue : errorDetails.values()) {
                fieldValue.retain();
            }
        }
        if (transactionErrorDetails != null) {
            synchronized (transactionErrorDetails) {
                for (CString fieldValue : transactionErrorDetails.values()) {
                    fieldValue.release();
                }
            }
        }
        transactionErrorDetails = errorDetails;
    }

    public boolean isInDatasetCreation() {
        return inDatasetCreation;
    }

    public void setInDatasetCreation(boolean inDatasetCreation) {
        this.inDatasetCreation = inDatasetCreation;
    }

    public Operation getCurrentCommandOperation() {
        return currentCommandOperation;
    }

    public void setCurrentCommandOperation(Operation operation) {
        this.currentCommandOperation = operation;
    }

    public Promise getPromise() {
        return promise;
    }

    public void setPromise(Promise promise) {
        this.promise = promise;
    }

    public TransferMode getTransferMode() {
        return transferMode;
    }

    public void setTransferMode(TransferMode transferMode) {
        this.transferMode = transferMode;
    }

    public List<Query> getBufferedQueries() {
        if (bufferedQueries == null) {
            bufferedQueries = Collections.synchronizedList(new ArrayList<>());
        }
        return bufferedQueries;
    }

    public void setBufferedQueries(List<Query> bufferedQueries) {
        this.bufferedQueries = bufferedQueries;
    }

    public int addBufferedQuery(Query query) {
        // Retain internal buffers of query
        query.retain();
        getBufferedQueries().add(query);
        return getBufferedQueries().size() - 1;
    }

    public void resetBufferedQueries() {
        if (bufferedQueries != null) {
            synchronized (bufferedQueries) {
                for (Query bufferedQuery : bufferedQueries) {
                    // Release internal buffers of query
                    bufferedQuery.release();
                }
                bufferedQueries.clear();
            }
        }
    }

    public List<PgsqlRowDescriptionMessage.Field> getRowDescription() {
        return this.rowDescription;
    }

    public void setRowDescription(List<PgsqlRowDescriptionMessage.Field> rowDescription) {
        // Retain internal buffer of each field name
        if (rowDescription != null) {
            for (PgsqlRowDescriptionMessage.Field field : rowDescription) {
                if (field.getName().isBuffered()) {
                    field.getName().retain();
                }
            }
        }
        this.rowDescription = rowDescription;
    }

    public void resetRowDescription() {
        if (rowDescription != null) {
            synchronized (rowDescription) {
                for (PgsqlRowDescriptionMessage.Field field : rowDescription) {
                    if (field.getName().release()) {
                        field.setName(null);
                    }
                }
            }
            rowDescription = null;
        }
    }

    public Deque<QueryResponseType> getQueryResponsesToIgnore() {
        if (queryResponsesToIgnore == null) {
            queryResponsesToIgnore = new ConcurrentLinkedDeque<>();
        }
        return queryResponsesToIgnore;
    }

    public void resetQueryResponsesToIgnore() {
        if (queryResponsesToIgnore != null) {
            queryResponsesToIgnore.clear();
        }
    }

    public void addFirstQueryResponseToIgnore(QueryResponseType queryResponseType) {
        getQueryResponsesToIgnore().addFirst(queryResponseType);
    }

    public void addLastQueryResponseToIgnore(QueryResponseType queryResponseType) {
        getQueryResponsesToIgnore().addLast(queryResponseType);
    }

    public QueryResponseType firstQueryResponseToIgnore() {
        return getQueryResponsesToIgnore().peekFirst();
    }

    public QueryResponseType lastQueryResponseToIgnore() {
        return getQueryResponsesToIgnore().peekLast();
    }

    public QueryResponseType removeFirstQueryResponseToIgnore() {
        return getQueryResponsesToIgnore().pollFirst();
    }

    public QueryResponseType removeLastQueryResponseToIgnore() {
        return getQueryResponsesToIgnore().pollLast();
    }

    public boolean isProcessingQuery() {
        return getCurrentCommandOperation() != null || !getBufferedQueries().isEmpty();
    }

    public Map<CString, ExtendedQueryStatus<ParseStep>> getParseStepStatuses() {
        if (parseStepStatuses == null) {
            parseStepStatuses = Collections.synchronizedMap(new HashMap<>());
        }
        return parseStepStatuses;
    }

    public void setParseStepStatuses(Map<CString, ExtendedQueryStatus<ParseStep>> parseStepStatuses) {
        this.parseStepStatuses = parseStepStatuses;
    }

    public ExtendedQueryStatus<ParseStep> addParseStep(ParseStep parseStep, Operation operation, boolean toProcess) {
        ExtendedQueryStatus<ParseStep> parseStepStatus = new ExtendedQueryStatus<>(parseStep, operation, toProcess);
        // Retain internal buffers of parse step status
        parseStepStatus.retain();
        ExtendedQueryStatus<ParseStep> previousParseStepStatus = getParseStepStatuses().put(parseStep.getName(),
                parseStepStatus);
        if (previousParseStepStatus != null) {
            // Release internal buffers of parse step status
            previousParseStepStatus.release();
        }
        return parseStepStatus;
    }

    public void removeParseStep(CString name) {
        // Remove describe if exist
        removeDescribeStep((byte) 'S', name);
        // Remove create portals if exist
        getBindStepStatuses().values().stream() // iterate over bind step status
                .map(ExtendedQueryStatus<BindStep>::getQuery) // retrieve bind
                                                              // step
                .filter(q -> name.equals(q.getPreparedStatement())) // filter on
                                                                    // prepared
                                                                    // statement
                                                                    // name
                .forEach(q -> removeBindStep(q.getName())); // remove the bind
                                                            // step
        // Remove parse step status
        ExtendedQueryStatus<ParseStep> parseStepStatus = getParseStepStatuses().remove(name);
        if (parseStepStatus != null) {
            // Release internal buffers of parse step status
            parseStepStatus.release();
        }
    }

    public void resetParseSteps() {
        if (parseStepStatuses != null) {
            synchronized (parseStepStatuses) {
                for (ExtendedQueryStatus<ParseStep> parseStepStatus : parseStepStatuses.values()) {
                    // Release internal buffers of parse step
                    parseStepStatus.getQuery().release();
                }
                parseStepStatuses.clear();
            }
        }
    }

    public ExtendedQueryStatus<ParseStep> getParseStepStatus(CString name) {
        return getParseStepStatuses().get(name);
    }

    public Map<CString, ExtendedQueryStatus<BindStep>> getBindStepStatuses() {
        if (bindStepStatuses == null) {
            bindStepStatuses = Collections.synchronizedMap(new HashMap<>());
        }
        return bindStepStatuses;
    }

    public void setBindStepStatuses(Map<CString, ExtendedQueryStatus<BindStep>> bindStepStatuses) {
        this.bindStepStatuses = bindStepStatuses;
    }

    public ExtendedQueryStatus<BindStep> addBindStep(BindStep bindStep, Operation operation, boolean toProcess) {
        ExtendedQueryStatus<BindStep> bindStepStatus = new ExtendedQueryStatus<>(bindStep, operation, toProcess);
        // Retain internal buffers of bind step status
        bindStepStatus.retain();
        ExtendedQueryStatus<BindStep> previousBindStepStatus = getBindStepStatuses().put(bindStep.getName(),
                bindStepStatus);
        if (previousBindStepStatus != null) {
            // Release internal buffers of bind step status
            previousBindStepStatus.release();
        }
        return bindStepStatus;
    }

    public void removeBindStep(CString name) {
        // Remove describe if exist
        removeDescribeStep((byte) 'P', name);
        // Remove bind step status
        ExtendedQueryStatus<BindStep> previousBindStepStatus = getBindStepStatuses().remove(name);
        if (previousBindStepStatus != null) {
            // Release internal buffers of bind step status
            previousBindStepStatus.release();
        }
    }

    public void resetBindSteps() {
        if (bindStepStatuses != null) {
            synchronized (bindStepStatuses) {
                for (ExtendedQueryStatus<BindStep> bindStepStatus : bindStepStatuses.values()) {
                    // Release internal buffers of bind step status
                    bindStepStatus.release();
                }
                bindStepStatuses.clear();
            }
        }
    }

    public ExtendedQueryStatus<BindStep> getBindStepStatus(CString name) {
        return getBindStepStatuses().get(name);
    }

    public Map<CString, DescribeStep> getDescribeSteps() {
        if (describeSteps == null) {
            describeSteps = Collections.synchronizedMap(new HashMap<>());
        }
        return describeSteps;
    }

    public void setDescribeSteps(Map<CString, DescribeStep> describeSteps) {
        this.describeSteps = describeSteps;
    }

    public DescribeStep addDescribeStep(DescribeStep describeStep) {
        // Retain internal buffers of describe step
        describeStep.retain();
        CString key = CString.valueOf((char) describeStep.getCode() + ":").append(describeStep.getName());
        DescribeStep previousDescribeStep = getDescribeSteps().put(key, describeStep);
        if (previousDescribeStep != null) {
            // Release internal buffers of describe step
            previousDescribeStep.release();
        }
        return describeStep;
    }

    public void removeDescribeStep(byte code, CString name) {
        CString key = CString.valueOf((char) code + ":").append(name);
        DescribeStep previousDescribeStep = getDescribeSteps().remove(key);
        if (previousDescribeStep != null) {
            // Release internal buffers of describe step
            previousDescribeStep.release();
        }
    }

    public void resetDescribeSteps() {
        if (describeSteps != null) {
            synchronized (describeSteps) {
                for (DescribeStep describeSteps : describeSteps.values()) {
                    // Release internal buffers of describe step
                    describeSteps.release();
                }
                describeSteps.clear();
            }
        }
    }

    public DescribeStep getDescribeStep(byte code, CString name) {
        CString key = CString.valueOf((char) code + ":").append(name);
        return getDescribeSteps().get(key);
    }

    public void resetCurrentCommand() {
        setCurrentCommandOperation(null);
        setPromise(null);
        resetRowDescription();
    }

    public void resetCurrentQueries() {
        resetCurrentCommand();
        resetBufferedQueries();
        resetQueryResponsesToIgnore();
    }

    public void reset() {
        setDatabaseName(null);
        setTransactionStatus((byte) 'I');
        setInDatasetCreation(false);
        setTransferMode(null);
        resetCurrentQueries();
    }

}
