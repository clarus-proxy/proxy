package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.dataoperations.Promise;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.Operation;
import io.netty.util.ReferenceCounted;

public class SQLSession {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLSession.class);
    private static final boolean FORCE_LEAK_DETECTION;
    static {
        String leakDetectionLevel = System.getProperty("io.netty.leakDetectionLevel", "SIMPLE");
        FORCE_LEAK_DETECTION = "PARANOID".equalsIgnoreCase(leakDetectionLevel);
    }

    private static final boolean CHECK_BUFFER_REFERENCE_COUNT;
    static {
        String bufferCheckReferenceCount = System.getProperty("buffer.check.reference.count", "false");
        CHECK_BUFFER_REFERENCE_COUNT = Boolean.TRUE.toString().equalsIgnoreCase(bufferCheckReferenceCount)
                || "1".equalsIgnoreCase(bufferCheckReferenceCount) || "yes".equalsIgnoreCase(bufferCheckReferenceCount)
                || "on".equalsIgnoreCase(bufferCheckReferenceCount);
    }

    public static class AuthenticationPhase {
        private final List<AuthenticationResponse> authenticationResponses;
        private final AtomicInteger receivedAuthenticationResponses;

        public AuthenticationPhase(int nbBackends) {
            this.authenticationResponses = new ArrayList<>(Collections.nCopies(nbBackends, null));
            this.receivedAuthenticationResponses = new AtomicInteger(0);
        }

        public int getNbAuthenticationResponses() {
            return authenticationResponses.size();
        }

        public synchronized AuthenticationResponse getAuthenticationResponse(int serverEndpoint) {
            return authenticationResponses.get(serverEndpoint);
        }

        public synchronized boolean setAndCountAuthenticationResponse(int serverEndpoint,
                AuthenticationResponse authenticationResponse) {
            setAuthenticationResponse(serverEndpoint, authenticationResponse);
            return areAllAuthenticationResponsesReceived();
        }

        public synchronized AuthenticationResponse setAuthenticationResponse(int backend,
                AuthenticationResponse authenticationResponse) {
            AuthenticationResponse previousAuthenticationResponse = authenticationResponses.set(backend,
                    authenticationResponse);
            if (previousAuthenticationResponse != null) {
                receivedAuthenticationResponses.decrementAndGet();
                if (previousAuthenticationResponse.getParameters() != null) {
                    // Release internal buffer
                    previousAuthenticationResponse.getParameters().release();
                }
            }
            if (authenticationResponse != null) {
                receivedAuthenticationResponses.incrementAndGet();
                if (authenticationResponse.getParameters() != null) {
                    // Retain internal buffer
                    authenticationResponse.getParameters().retain();
                }
            }
            return previousAuthenticationResponse;
        }

        public synchronized void clear() {
            for (int i = 0; i < authenticationResponses.size(); i++) {
                setAuthenticationResponse(i, null);
            }
        }

        public synchronized boolean areAllAuthenticationResponsesReceived() {
            return receivedAuthenticationResponses.get() == authenticationResponses.size();
        }

        public synchronized void waitForAllResponses() throws IOException {
            if (areAllAuthenticationResponsesReceived()) {
                return;
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("waiting authentication responses ({})", System.identityHashCode(this));
            }
            try {
                wait();
            } catch (InterruptedException e) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("unexpected thread interruption", e);
                }
                throw new IOException(e);
            }
            if (!areAllAuthenticationResponsesReceived()) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("unexpected: missing authentication responses");
                }
                throw new IllegalStateException("unexpected: missing authentication responses");
            } else if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("authentication responses have been received ({})", System.identityHashCode(this));
            }
        }

        public synchronized void allResponsesReceived() {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("notifying authentication responses have been received ({})",
                        System.identityHashCode(this));
            }
            notifyAll();
        }
    }

    public static abstract class AbsractExpectedField {
        protected String name;
        protected int position;
        protected List<Map.Entry<String, Integer>> attributes;

        public AbsractExpectedField(String name) {
            this(name, null);
        }

        public AbsractExpectedField(String name, List<String> attributeNames) {
            this(name, -1, new ArrayList<>());
            if (attributeNames != null) {
                addAttributeNames(attributeNames);
            }
        }

        public AbsractExpectedField(String name, int position, List<Map.Entry<String, Integer>> attributes) {
            this.name = name;
            this.position = position;
            this.attributes = attributes;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        public List<Map.Entry<String, Integer>> getAttributes() {
            return attributes;
        }

        public void setAttributes(List<Map.Entry<String, Integer>> attributes) {
            this.attributes = attributes;
        }

        public void addAttributeNames(List<String> attributeNames) {
            attributes
                    .addAll(attributeNames.stream().map(an -> new SimpleEntry<>(an, -1)).collect(Collectors.toList()));
        }
    }

    public static class ExpectedProtectedField extends AbsractExpectedField {
        private final int backend;
        private List<Map.Entry<String, Integer>> attributeMapping;

        public ExpectedProtectedField(int backend, String name) {
            super(name);
            this.backend = backend;
            this.attributeMapping = new ArrayList<>();
        }

        public ExpectedProtectedField(int backend, String name, List<String> attributeNames,
                List<Map.Entry<String, Integer>> attributeMapping) {
            super(name, attributeNames);
            this.backend = backend;
            this.attributeMapping = attributeMapping;
        }

        public ExpectedProtectedField(int backend, String name, int position,
                List<Map.Entry<String, Integer>> attributes, List<Map.Entry<String, Integer>> attributeMapping) {
            super(name, position, attributes);
            this.backend = backend;
            this.attributeMapping = attributeMapping;
        }

        public int getBackend() {
            return backend;
        }

        public List<Map.Entry<String, Integer>> getAttributeMapping() {
            return attributeMapping;
        }

        public void setAttributeMapping(List<Map.Entry<String, Integer>> attributeMapping) {
            this.attributeMapping = attributeMapping;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("ExpectedProtectedField [backend=");
            builder.append(backend);
            builder.append(", name=");
            builder.append(name);
            builder.append(", position=");
            builder.append(position);
            builder.append(", attributes=");
            builder.append(attributes);
            builder.append(", attributeMapping=");
            builder.append(attributeMapping);
            builder.append("]");
            return builder.toString();
        }
    }

    public static class ExpectedField extends AbsractExpectedField {
        private Map<Integer, List<ExpectedProtectedField>> protectedFields;

        public ExpectedField(String name) {
            this(name, null, new HashMap<>());
        }

        public ExpectedField(String name, List<String> attributeNames,
                Map<Integer, List<ExpectedProtectedField>> protectedFields) {
            super(name, attributeNames);
            this.protectedFields = protectedFields;
        }

        public ExpectedField(String name, int position, List<Map.Entry<String, Integer>> attributes,
                Map<Integer, List<ExpectedProtectedField>> protectedFields) {
            super(name, position, attributes);
            this.protectedFields = protectedFields;
        }

        public Map<Integer, List<ExpectedProtectedField>> getProtectedFields() {
            return protectedFields;
        }

        public void setProtectedFields(Map<Integer, List<ExpectedProtectedField>> protectedFields) {
            this.protectedFields = protectedFields;
        }

        public List<ExpectedProtectedField> getBackendProtectedFields(int backend) {
            return protectedFields.get(backend);
        }

        public void setBackendProtectedFields(int backend, List<ExpectedProtectedField> backendProtectedFields) {
            protectedFields.put(backend, backendProtectedFields);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("ExpectedField [name=");
            builder.append(name);
            builder.append(", position=");
            builder.append(position);
            builder.append(", attributes=");
            builder.append(attributes);
            builder.append(", protectedFields=");
            builder.append(protectedFields);
            builder.append("]");
            return builder.toString();
        }
    }

    public static class ExtendedQueryStatus<Q extends ExtendedQuery> {
        private final Q query;
        private final Operation operation;
        private final SQLCommandType type;
        private final boolean toProcess;
        private boolean processed;
        private List<Integer> involvedBackends;

        public ExtendedQueryStatus(Q query, Operation operation, SQLCommandType type) {
            this(query, operation, type, false, Collections.singletonList(0));
        }

        public ExtendedQueryStatus(Q query, Operation operation, SQLCommandType type, boolean toProcess,
                List<Integer> involvedBackends) {
            this.query = query;
            this.operation = operation;
            this.type = type;
            this.toProcess = toProcess;
            this.processed = false;
            this.involvedBackends = involvedBackends;
        }

        public Q getQuery() {
            return query;
        }

        public Operation getOperation() {
            return operation;
        }

        public SQLCommandType getType() {
            return type;
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

        public List<Integer> getInvolvedBackends() {
            return involvedBackends;
        }

        public void setInvolvedBackends(List<Integer> involvedBackends) {
            this.involvedBackends = involvedBackends;
        }

        public void retain() {
            query.retain();
        }

        public boolean release() {
            return query.release();
        }
    }

    public static class DescribeStepStatus extends ExtendedQueryStatus<DescribeStep> {
        private List<ExpectedField> expectedFields;
        private SortedMap<Integer, List<PgsqlRowDescriptionMessage.Field>> backendRowDescriptions;
        private List<PgsqlRowDescriptionMessage.Field> rowDescription;

        public DescribeStepStatus(DescribeStep describeStep, Operation operation, SQLCommandType type) {
            super(describeStep, operation, type);
        }

        public DescribeStepStatus(DescribeStep describeStep, Operation operation, SQLCommandType type,
                boolean toProcess, List<Integer> involvedBackends) {
            super(describeStep, operation, type, toProcess, involvedBackends);
        }

        public List<ExpectedField> getExpectedFields() {
            return expectedFields;
        }

        public void setExpectedFields(List<ExpectedField> expectedFields) {
            this.expectedFields = expectedFields;
        }

        public SortedMap<Integer, List<PgsqlRowDescriptionMessage.Field>> getBackendRowDescriptions() {
            return backendRowDescriptions;
        }

        public void setBackendRowDescriptions(
                SortedMap<Integer, List<PgsqlRowDescriptionMessage.Field>> backendRowDescriptions) {
            // Ensure field name is accessible as a String
            if (backendRowDescriptions != null) {
                for (List<PgsqlRowDescriptionMessage.Field> fields : backendRowDescriptions.values()) {
                    for (PgsqlRowDescriptionMessage.Field field : fields) {
                        field.getName().toString();
                    }
                }
            }
            this.backendRowDescriptions = backendRowDescriptions;
        }

        public List<PgsqlRowDescriptionMessage.Field> getRowDescription() {
            return rowDescription;
        }

        public void setRowDescription(List<PgsqlRowDescriptionMessage.Field> rowDescription) {
            // Ensure field name is accessible as a String
            if (rowDescription != null) {
                for (PgsqlRowDescriptionMessage.Field field : rowDescription) {
                    field.getName().toString();
                }
            }
            this.rowDescription = rowDescription;
        }
    }

    public enum QueryResponseType {
        PARSE_COMPLETE,
        BIND_COMPLETE,
        PARAMETER_DESCRIPTION,
        ROW_DESCRIPTION,
        DATA_ROW,
        NO_DATA,
        ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA,
        COMMAND_COMPLETE,
        EMPTY_QUERY,
        PORTAL_SUSPENDED,
        ERROR,
        COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR,
        CLOSE_COMPLETE,
        READY_FOR_QUERY
    }

    public static class QueryResponseStatus<T> {
        private final QueryResponseType type;
        private final Map<Integer, Queue<T>> detailsPerBackend;
        private final int nbBackends;

        public QueryResponseStatus(QueryResponseType type, int nbBackends) {
            this.type = type;
            this.detailsPerBackend = new ConcurrentHashMap<>(nbBackends);
            this.nbBackends = nbBackends;
        }

        private Queue<T> getDetails(int backend) {
            return detailsPerBackend.computeIfAbsent(backend, nil -> new LinkedList<>());
        }

        public synchronized SortedMap<Integer, T> addAndRemove(int backend, T details) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("adding {} response {} for backend {}/{}", type, System.identityHashCode(details),
                        backend + 1, nbBackends);
            }
            retain(details);
            synchronized (this) {
                getDetails(backend).add(details);
                return remove();
            }
        }

        public synchronized SortedMap<Integer, T> addAndRemove(int backend, T details,
                Map<Integer, List<Integer>> detailIndexesPerBackend) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("adding {} response {} for backend {}/{}", type, System.identityHashCode(details),
                        backend + 1, nbBackends);
            }
            retain(details);
            synchronized (this) {
                getDetails(backend).add(details);
                return remove(detailIndexesPerBackend);
            }
        }

        public synchronized void clear() {
            for (Map.Entry<Integer, Queue<T>> entry : detailsPerBackend.entrySet()) {
                Queue<T> backendDetails = entry.getValue();
                if (backendDetails.size() > 0) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("strange: {} remaining {} responses of backend {}/{} are to be ignored",
                                backendDetails.size(), type, entry.getKey() + 1, nbBackends);
                    }
                } else {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("{} remaining {} responses of backend {}/{} are to be ignored",
                                backendDetails.size(), type, entry.getKey() + 1, nbBackends);
                    }
                }
                for (T details : backendDetails) {
                    release(details);
                }
            }
            detailsPerBackend.clear();
        }

        public synchronized SortedMap<Integer, T> remove() {
            if (!available()) {
                return null;
            }
            boolean allEmpty = true;
            SortedMap<Integer, T> firstDetailsPerBackend = new TreeMap<>();
            for (Map.Entry<Integer, Queue<T>> entry : detailsPerBackend.entrySet()) {
                Integer backend = entry.getKey();
                Queue<T> backendDetails = entry.getValue();
                T details = backendDetails.remove();
                allEmpty &= backendDetails.isEmpty();
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("removing {} response {} of backend {}/{}", type, System.identityHashCode(details),
                            backend + 1, nbBackends);
                }
                firstDetailsPerBackend.put(backend, details);
            }
            if (allEmpty) {
                detailsPerBackend.clear();
            }
            return firstDetailsPerBackend;
        }

        private SortedMap<Integer, T> remove(Map<Integer, List<Integer>> detailIndexesPerBackend) {
            if (nbBackends == 1 || detailIndexesPerBackend == null) {
                return remove();
            }
            if (!available()) {
                return null;
            }
            List<Queue<T>> backendDetails = detailsPerBackend.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .map(Map.Entry::getValue).collect(Collectors.toList());
            int matchingBackends = 0;
            @SuppressWarnings("unchecked")
            Iterator<T>[] iters = new Iterator[nbBackends];
            @SuppressWarnings("unchecked")
            T[] backendsDetails = (T[]) new Object[nbBackends];
            List<Integer> backends = detailIndexesPerBackend.keySet().stream().sorted().collect(Collectors.toList());
            for (iters[0] = backendDetails.get(0).iterator(); iters[0].hasNext();) {
                backendsDetails[0] = iters[0].next();
                List<?> details1Item;
                if (backendsDetails[0] instanceof List) {
                    int backend = backends.get(0);
                    List<?> backendDetails2 = (List<?>) backendsDetails[backend];
                    List<Integer> backendDetailIndexes = detailIndexesPerBackend.get(backend);
                    details1Item = backendDetailIndexes.stream().map(i -> backendDetails2.get(i))
                            .collect(Collectors.toList());
                } else {
                    details1Item = Collections.singletonList(backendsDetails[0]);
                }
                matchingBackends++;
                for (int b = 1; b < nbBackends; b++) {
                    for (iters[b] = backendDetails.get(b).iterator(); iters[b].hasNext();) {
                        backendsDetails[b] = iters[b].next();
                        List<?> details2Item;
                        if (backendsDetails[b] instanceof List) {
                            int backend = backends.get(b);
                            List<?> backendDetails2 = (List<?>) backendsDetails[backend];
                            List<Integer> backendDetailIndexes = detailIndexesPerBackend.get(backend);
                            details2Item = backendDetailIndexes.stream().map(i -> backendDetails2.get(i))
                                    .collect(Collectors.toList());
                        } else {
                            details2Item = Collections.singletonList(backendsDetails[b]);
                        }
                        if (details1Item == details2Item
                                || (details1Item != null && details1Item.equals(details2Item))) {
                            matchingBackends++;
                            break;
                        }
                    }
                    if (matchingBackends < b + 1) {
                        break;
                    }
                }
                if (matchingBackends == nbBackends) {
                    break;
                }
                matchingBackends = 0;
            }
            if (matchingBackends == 0) {
                return null;
            }
            SortedMap<Integer, T> matchingDetailsPerBackend = new TreeMap<>();
            for (int b = 0; b < nbBackends; b++) {
                matchingDetailsPerBackend.put(b, backendsDetails[b]);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("removing {} response {} of backend {}/{}", type,
                            System.identityHashCode(backendsDetails[b]), b + 1, nbBackends);
                }
                iters[b].remove();
            }
            boolean allEmpty = detailsPerBackend.values().stream().allMatch(q -> q.isEmpty());
            if (allEmpty) {
                detailsPerBackend.clear();
            }
            return matchingDetailsPerBackend;
        }

        private boolean available() {
            boolean available = detailsPerBackend.size() == nbBackends;
            if (available) {
                for (Queue<T> backendDetails : detailsPerBackend.values()) {
                    available &= backendDetails != null && !backendDetails.isEmpty();
                }
            }
            return available;
        }

        private void retain(T details) {
            if (details instanceof ReferenceCounted) {
                if (CHECK_BUFFER_REFERENCE_COUNT && LOGGER.isTraceEnabled()) {
                    LOGGER.trace("retaining {} ({}), refcount={}", details, System.identityHashCode(details),
                            ((ReferenceCounted) details).refCnt());
                }
                ((ReferenceCounted) details).retain();
            } else if (details instanceof Collection) {
                for (Object responseDetail : (Collection<?>) details) {
                    if (responseDetail instanceof ReferenceCounted) {
                        if (CHECK_BUFFER_REFERENCE_COUNT && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("retaining {} ({}), refcount={}", responseDetail,
                                    System.identityHashCode(responseDetail),
                                    ((ReferenceCounted) responseDetail).refCnt());
                        }
                        ((ReferenceCounted) responseDetail).retain();
                    }
                }
            } else if (details instanceof Map) {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) details).entrySet()) {
                    Object key = entry.getKey();
                    if (key instanceof ReferenceCounted) {
                        if (CHECK_BUFFER_REFERENCE_COUNT && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("retaining {} ({}), refcount={}", key, System.identityHashCode(key),
                                    ((ReferenceCounted) key).refCnt());
                        }
                        ((ReferenceCounted) key).retain();
                    }
                    Object value = entry.getValue();
                    if (value instanceof ReferenceCounted) {
                        if (CHECK_BUFFER_REFERENCE_COUNT && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("retaining {} ({}), refcount={}", value, System.identityHashCode(value),
                                    ((ReferenceCounted) value).refCnt());
                        }
                        ((ReferenceCounted) value).retain();
                    }
                }
            }
        }

        private void release(T details) {
            if (details instanceof ReferenceCounted) {
                if (CHECK_BUFFER_REFERENCE_COUNT && LOGGER.isTraceEnabled()) {
                    LOGGER.trace("releasing {} ({}), refcount={}", details, System.identityHashCode(details),
                            ((ReferenceCounted) details).refCnt());
                }
                ((ReferenceCounted) details).release();
            } else if (details instanceof Collection) {
                for (Object responseDetail : (Collection<?>) details) {
                    if (responseDetail instanceof ReferenceCounted) {
                        if (CHECK_BUFFER_REFERENCE_COUNT && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("releasing {} ({}), refcount={}", responseDetail,
                                    System.identityHashCode(responseDetail),
                                    ((ReferenceCounted) responseDetail).refCnt());
                        }
                        ((ReferenceCounted) responseDetail).release();
                    }
                }
            } else if (details instanceof Map) {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) details).entrySet()) {
                    Object key = entry.getKey();
                    if (key instanceof ReferenceCounted) {
                        if (CHECK_BUFFER_REFERENCE_COUNT && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("releasing {} ({}), refcount={}", key, System.identityHashCode(key),
                                    ((ReferenceCounted) key).refCnt());
                        }
                        ((ReferenceCounted) key).release();
                    }
                    Object value = entry.getValue();
                    if (value instanceof ReferenceCounted) {
                        if (CHECK_BUFFER_REFERENCE_COUNT && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("releasing {} ({}), refcount={}", value, System.identityHashCode(value),
                                    ((ReferenceCounted) value).refCnt());
                        }
                        ((ReferenceCounted) value).release();
                    }
                }
            }
        }
    }

    public static class CursorContext {
        private final String name;
        private final Promise promise;
        private final boolean resultProcessingEnabled;
        private final boolean unprotectingDataEnabled;
        private final List<Integer> involvedBackends;
        private final List<ExpectedField> expectedFields;

        public CursorContext(String name, Promise promise, boolean resultProcessingEnabled,
                boolean unprotectingDataEnabled, List<Integer> involvedBackends, List<ExpectedField> expectedFields) {
            this.name = name;
            this.promise = promise;
            this.resultProcessingEnabled = resultProcessingEnabled;
            this.unprotectingDataEnabled = unprotectingDataEnabled;
            this.involvedBackends = involvedBackends;
            this.expectedFields = expectedFields;
        }

        public String getName() {
            return name;
        }

        public Promise getPromise() {
            return promise;
        }

        public boolean isResultProcessingEnabled() {
            return resultProcessingEnabled;
        }

        public boolean isUnprotectingDataEnabled() {
            return unprotectingDataEnabled;
        }

        public List<Integer> getInvolvedBackends() {
            return involvedBackends;
        }

        public List<ExpectedField> getExpectedFields() {
            return expectedFields;
        }
    }

    private String user;
    private String databaseName;
    private AuthenticationPhase authenticationPhase;
    private byte transactionStatus = (byte) 'I';
    private Map<Byte, CString> transactionErrorDetails;
    private boolean inDatasetCreation;
    private Operation currentCommandOperation;
    private Promise promise;
    private boolean resultProcessingEnabled = true;
    private boolean unprotectingDataEnabled = false;
    private Map<CString, List<CString>> datasetDefinitions;
    private Deque<List<Integer>> commandsInvolvedBackends;
    private List<Integer> queryInvolvedBackends;
    private List<ExpectedField> expectedFields;
    private boolean tableDefinitionEnabled;
    private Map<String, String> rowDescriptionFieldsToTrack;
    private TransferMode transferMode;
    private List<Query> bufferedQueries;
    private Map<Map.Entry<QueryResponseType, Integer>, QueryResponseStatus<?>> queryResponses;
    private SortedMap<Integer, List<PgsqlRowDescriptionMessage.Field>> backendRowDescriptions;
    private List<PgsqlRowDescriptionMessage.Field> rowDescription;
    private Map<Integer, SortedSet<Integer>> tableOIDBackends;
    private Map<Long, SortedSet<Integer>> typeOIDBackends;
    private Map<Integer, List<Integer>> joinFieldIndexes;
    private Deque<QueryResponseType> queryResponsesToIgnore;
    private SortedMap<CString, ExtendedQueryStatus<ParseStep>> parseStepStatuses;
    private SortedMap<CString, ExtendedQueryStatus<BindStep>> bindStepStatuses;
    private SortedMap<CString, DescribeStepStatus> describeStepStatuses;
    private CString currentDescribeStepKey;
    private SortedMap<String, CursorContext> cursorContexts;
    private CountDownLatch responsesReceived = null;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public AuthenticationPhase getAuthenticationPhase() {
        return authenticationPhase;
    }

    public void setAuthenticationPhase(AuthenticationPhase authenticationPhase) {
        if (this.authenticationPhase != null) {
            this.authenticationPhase.clear();
        }
        this.authenticationPhase = authenticationPhase;
    }

    public byte getTransactionStatus() {
        return transactionStatus;
    }

    public void setTransactionStatus(byte transactionStatus) {
        this.transactionStatus = transactionStatus;
    }

    public Map<Byte, CString> getTransactionErrorDetails() {
        return transactionErrorDetails;
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

    public boolean isResultProcessingEnabled() {
        return resultProcessingEnabled;
    }

    public void setResultProcessingEnabled(boolean resultProcessingEnabled) {
        this.resultProcessingEnabled = resultProcessingEnabled;
    }

    public boolean isUnprotectingDataEnabled() {
        return unprotectingDataEnabled;
    }

    public void setUnprotectingDataEnabled(boolean unprotectingDataEnabled) {
        this.unprotectingDataEnabled = unprotectingDataEnabled;
    }

    public Map<CString, List<CString>> getDatasetDefinitions() {
        if (datasetDefinitions == null) {
            datasetDefinitions = Collections.synchronizedMap(new HashMap<>());
        }
        return datasetDefinitions;
    }

    public void setDatasetDefinitions(Map<CString, List<CString>> datasetDefinitions) {
        this.datasetDefinitions = datasetDefinitions;
    }

    public void addDatasetDefinition(CString datasetId, List<CString> dataIds) {
        getDatasetDefinitions().put(datasetId, dataIds);
    }

    public void removeDatasetDefinition(CString datasetId) {
        getDatasetDefinitions().remove(datasetId);
    }

    public void resetDatasetDefinition() {
        if (datasetDefinitions != null) {
            datasetDefinitions.clear();
        }
    }

    public List<CString> getDatasetDefinition(CString datasetId) {
        return getDatasetDefinitions().get(datasetId);
    }

    public synchronized List<Integer> getCommandInvolvedBackends() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("getting first of commands involved backends: {}", commandsInvolvedBackends);
        }
        return commandsInvolvedBackends != null ? commandsInvolvedBackends.peek() : null;
    }

    public synchronized void setCommandInvolvedBackends(List<Integer> involvedBackends) {
        setCommandInvolvedBackends(involvedBackends, true);
    }

    public synchronized void setCommandInvolvedBackends(List<Integer> involvedBackends, boolean overwrite) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("setting {} command involved backends (overwite={})", involvedBackends, overwrite);
        }
        if (involvedBackends == null) {
            if (commandsInvolvedBackends != null) {
                commandsInvolvedBackends.poll();
                if (commandsInvolvedBackends.size() == 0) {
                    commandsInvolvedBackends = null;
                }
            }
        } else {
            if (overwrite && this.commandsInvolvedBackends != null) {
                commandsInvolvedBackends.pollLast();
            }
            if (commandsInvolvedBackends == null) {
                commandsInvolvedBackends = new ConcurrentLinkedDeque<>();
            }
            commandsInvolvedBackends.add(involvedBackends);
            queryInvolvedBackends = commandsInvolvedBackends.peek();
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("after setting commands involved backends: {}", commandsInvolvedBackends);
            LOGGER.trace("and query involved backends: {}", queryInvolvedBackends);
        }
    }

    public synchronized List<Integer> getQueryInvolvedBackends() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("getting query involved backends: {}", queryInvolvedBackends);
        }
        return queryInvolvedBackends;
    }

    public synchronized void setQueryInvolvedBackends(List<Integer> involvedBackends) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("setting {} query involved backends", involvedBackends);
        }
        if (involvedBackends == null && commandsInvolvedBackends != null) {
            queryInvolvedBackends = commandsInvolvedBackends.peek();
        } else {
            queryInvolvedBackends = involvedBackends;
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("after setting query involved backends: {}", queryInvolvedBackends);
        }
    }

    public List<ExpectedField> getExpectedFields() {
        return expectedFields;
    }

    public void setExpectedFields(List<ExpectedField> expectedFields) {
        this.expectedFields = expectedFields;
    }

    public void setTableDefinitionEnabled(boolean tableDefinitionEnabled) {
        this.tableDefinitionEnabled = tableDefinitionEnabled;
        if (!tableDefinitionEnabled) {
            resetRowDescriptionFieldsToTrack();
        }
    }

    public boolean isTableDefinitionEnabled() {
        return tableDefinitionEnabled;
    }

    public Map<String, String> getRowDescriptionFieldsToTrack() {
        if (rowDescriptionFieldsToTrack == null) {
            rowDescriptionFieldsToTrack = Collections.synchronizedMap(new HashMap<>());
        }
        return rowDescriptionFieldsToTrack;
    }

    public void addRowDescriptionFieldToTrack(String fieldName, String columnName) {
        getRowDescriptionFieldsToTrack().put(fieldName, columnName);
    }

    public void resetRowDescriptionFieldsToTrack() {
        if (rowDescriptionFieldsToTrack != null) {
            rowDescriptionFieldsToTrack.clear();
        }
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

    public synchronized Map<Map.Entry<QueryResponseType, Integer>, QueryResponseStatus<?>> getQueryResponses() {
        if (queryResponses == null) {
            queryResponses = new ConcurrentHashMap<>();
        }
        return queryResponses;
    }

    public void resetQueryResponses() {
        if (queryResponses != null) {
            for (QueryResponseStatus<?> queryResponse : queryResponses.values()) {
                queryResponse.clear();
            }
            queryResponses.clear();
        }
    }

    public <T> SortedMap<Integer, T> newQueryResponse(QueryResponseType type, int backend, int nbBackends, T details) {
        if (type == null) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("type cannot be null");
            }
            throw new NullPointerException("type cannot be null");
        }
        if (nbBackends <= 0) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("nbBackends ({}) must be positive", nbBackends);
            }
            throw new IllegalArgumentException(String.format("nbBackends (%d) must be positive", nbBackends));
        }
        // don't test backend against nbBackends since it can be greater
        // e.g. a response from one backend which is the 2nd backend
        if (backend < 0) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("backend ({}) must be positive or zero", backend);
            }
            throw new IllegalArgumentException(String.format("backend (%d) must be positive or zero", backend));
        }
        Map.Entry<QueryResponseType, Integer> key = new SimpleEntry<>(type, nbBackends);
        @SuppressWarnings("unchecked")
        QueryResponseStatus<T> queryResponses = (QueryResponseStatus<T>) getQueryResponses().computeIfAbsent(key,
                nil -> new QueryResponseStatus<T>(type, nbBackends));
        return queryResponses.addAndRemove(backend, details);
    }

    public <T> SortedMap<Integer, T> newQueryResponse(QueryResponseType type, int backend, int nbBackends, T details,
            Map<Integer, List<Integer>> detailsIndexesPerBackend) {
        if (type == null) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("type cannot be null");
            }
            throw new NullPointerException("type cannot be null");
        }
        if (nbBackends <= 0) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("nbBackends ({}) must be positive", nbBackends);
            }
            throw new IllegalArgumentException(String.format("nbBackends (%d) must be positive", nbBackends));
        }
        // don't test backend against nbBackends since it can be greater
        // e.g. a response from one backend which is the 2nd backend
        if (backend < 0) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("backend ({}) must be positive or zero", backend);
            }
            throw new IllegalArgumentException(String.format("backend (%d) must be positive or zero", backend));
        }
        Map.Entry<QueryResponseType, Integer> key = new SimpleEntry<>(type, nbBackends);
        @SuppressWarnings("unchecked")
        QueryResponseStatus<T> queryResponses = (QueryResponseStatus<T>) getQueryResponses().computeIfAbsent(key,
                nil -> new QueryResponseStatus<T>(type, nbBackends));
        return queryResponses.addAndRemove(backend, details, detailsIndexesPerBackend);
    }

    public <T> SortedMap<Integer, T> nextQueryResponses(QueryResponseType type) {
        if (type == null) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("type cannot be null");
            }
            throw new NullPointerException("type cannot be null");
        }
        List<QueryResponseStatus<?>> candidates = getQueryResponses().entrySet().stream()
                .filter(e -> e.getKey().getKey() == type).map(Map.Entry::getValue).collect(Collectors.toList());
        for (QueryResponseStatus<?> candidate : candidates) {
            @SuppressWarnings("unchecked")
            QueryResponseStatus<T> queryResponses = (QueryResponseStatus<T>) candidate;
            SortedMap<Integer, T> nextQueryResponses = queryResponses.remove();
            if (nextQueryResponses != null) {
                return nextQueryResponses;
            }
        }
        return null;
    }

    public SortedMap<Integer, List<PgsqlRowDescriptionMessage.Field>> getBackendRowDescriptions() {
        return backendRowDescriptions;
    }

    public void setBackendRowDescriptions(
            SortedMap<Integer, List<PgsqlRowDescriptionMessage.Field>> backendRowDescriptions) {
        // Retain internal buffer of each field name
        if (backendRowDescriptions != null) {
            for (List<PgsqlRowDescriptionMessage.Field> fields : backendRowDescriptions.values()) {
                for (PgsqlRowDescriptionMessage.Field field : fields) {
                    field.retain();
                }
            }
        }
        this.backendRowDescriptions = backendRowDescriptions;
    }

    public void resetBackendRowDescriptions() {
        SortedMap<Integer, List<PgsqlRowDescriptionMessage.Field>> backendRowDescriptions = this.backendRowDescriptions;
        this.backendRowDescriptions = null;
        if (backendRowDescriptions != null) {
            for (List<PgsqlRowDescriptionMessage.Field> fields : backendRowDescriptions.values()) {
                for (PgsqlRowDescriptionMessage.Field field : fields) {
                    field.release();
                }
            }
            if (FORCE_LEAK_DETECTION) {
                for (List<PgsqlRowDescriptionMessage.Field> fields : backendRowDescriptions.values()) {
                    for (PgsqlRowDescriptionMessage.Field field : fields) {
                        if (field.refCnt() > 0) {
                            if (this.rowDescription != null && !this.rowDescription.contains(field)) {
                                try {
                                    Thread.sleep(10);
                                } catch (InterruptedException e) {
                                    // Should not occur
                                }
                                if (field.refCnt() > 0) {
                                    LOGGER.trace("potential memory leak ({} refs) detected for {}", field.refCnt(),
                                            field);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public Map<Integer, List<Integer>> getJoinFieldIndexes() {
        return this.joinFieldIndexes;
    }

    public void setJoinFieldIndexes(Map<Integer, List<Integer>> joinFieldIndexes) {
        this.joinFieldIndexes = joinFieldIndexes;
    }

    public List<PgsqlRowDescriptionMessage.Field> getRowDescription() {
        return rowDescription;
    }

    public void setRowDescription(List<PgsqlRowDescriptionMessage.Field> rowDescription) {
        // Retain internal buffer of each field name
        if (rowDescription != null) {
            for (PgsqlRowDescriptionMessage.Field field : rowDescription) {
                field.retain();
            }
        }
        this.rowDescription = rowDescription;
    }

    public void resetRowDescription() {
        List<PgsqlRowDescriptionMessage.Field> rowDescription = this.rowDescription;
        this.rowDescription = null;
        if (rowDescription != null) {
            for (PgsqlRowDescriptionMessage.Field field : rowDescription) {
                field.release();
            }
            if (FORCE_LEAK_DETECTION) {
                for (PgsqlRowDescriptionMessage.Field field : rowDescription) {
                    if (field.refCnt() > 0) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            // Should not occur
                        }
                        if (field.refCnt() > 0) {
                            LOGGER.trace("potential memory leak ({} refs) detected for {}", field.refCnt(), field);
                        }
                    }
                }
            }
        }
    }

    public Map<Integer, SortedSet<Integer>> getTableOIDBackends() {
        if (tableOIDBackends == null) {
            tableOIDBackends = new ConcurrentHashMap<>();
        }
        return tableOIDBackends;
    }

    public void setTableOIDBackends(Map<Integer, SortedSet<Integer>> tableOIDBackends) {
        this.tableOIDBackends = tableOIDBackends;
    }

    public void addTableOIDBackend(int tableOID, int backend) {
        SortedSet<Integer> backends = getTableOIDBackends().get(tableOID);
        if (backends == null) {
            synchronized (getTableOIDBackends()) {
                backends = getTableOIDBackends().get(tableOID);
                if (backends == null) {
                    backends = new TreeSet<>();
                    getTableOIDBackends().put(tableOID, backends);
                }
            }
        }
        backends.add(backend);
    }

    public SortedSet<Integer> getTableOIDBackends(int tableOID) {
        return getTableOIDBackends().get(tableOID);
    }

    public Map<Long, SortedSet<Integer>> getTypeOIDBackends() {
        if (typeOIDBackends == null) {
            typeOIDBackends = new ConcurrentHashMap<>();
        }
        return typeOIDBackends;
    }

    public void setTypeOIDBackends(Map<Long, SortedSet<Integer>> typeOIDBackends) {
        this.typeOIDBackends = typeOIDBackends;
    }

    public void addTypeOIDBackend(long typeOID, int backend) {
        SortedSet<Integer> backends = getTypeOIDBackends().get(typeOID);
        if (backends == null) {
            synchronized (getTypeOIDBackends()) {
                backends = getTypeOIDBackends().get(typeOID);
                if (backends == null) {
                    backends = new TreeSet<>();
                    getTypeOIDBackends().put(typeOID, backends);
                }
            }
        }
        backends.add(backend);
    }

    public SortedSet<Integer> getTypeOIDBackends(long typeOID) {
        return getTypeOIDBackends().get(typeOID);
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

    public SortedMap<CString, ExtendedQueryStatus<ParseStep>> getParseStepStatuses() {
        if (parseStepStatuses == null) {
            parseStepStatuses = Collections.synchronizedSortedMap(new TreeMap<>());
        }
        return parseStepStatuses;
    }

    public void setParseStepStatuses(SortedMap<CString, ExtendedQueryStatus<ParseStep>> parseStepStatuses) {
        this.parseStepStatuses = parseStepStatuses;
    }

    public ExtendedQueryStatus<ParseStep> addParseStep(ParseStep parseStep, Operation operation, SQLCommandType type,
            boolean toProcess, List<Integer> involvedBackends) {
        ExtendedQueryStatus<ParseStep> parseStepStatus = new ExtendedQueryStatus<>(parseStep, operation, type,
                toProcess, involvedBackends);
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
                .filter(q -> name.equals(q.getPreparedStatement())) // filter on prepared statement name
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

    public ExtendedQueryStatus<ParseStep> getLastParseStepStatus() {
        CString name = getParseStepStatuses().lastKey();
        return name != null ? getParseStepStatuses().get(name) : null;
    }

    public SortedMap<CString, ExtendedQueryStatus<BindStep>> getBindStepStatuses() {
        if (bindStepStatuses == null) {
            bindStepStatuses = Collections.synchronizedSortedMap(new TreeMap<>());
        }
        return bindStepStatuses;
    }

    public void setBindStepStatuses(SortedMap<CString, ExtendedQueryStatus<BindStep>> bindStepStatuses) {
        this.bindStepStatuses = bindStepStatuses;
    }

    public ExtendedQueryStatus<BindStep> addBindStep(BindStep bindStep, Operation operation, SQLCommandType type,
            boolean toProcess, List<Integer> involvedBackends) {
        ExtendedQueryStatus<BindStep> bindStepStatus = new ExtendedQueryStatus<>(bindStep, operation, type, toProcess,
                involvedBackends);
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

    public ExtendedQueryStatus<BindStep> getLastBindStepStatus() {
        CString name = getBindStepStatuses().lastKey();
        return name != null ? getBindStepStatuses().get(name) : null;
    }

    public SortedMap<CString, DescribeStepStatus> getDescribeStepStatuses() {
        if (describeStepStatuses == null) {
            describeStepStatuses = Collections.synchronizedSortedMap(new TreeMap<>());
        }
        return describeStepStatuses;
    }

    public void setDescribeStepStatuses(SortedMap<CString, DescribeStepStatus> describeStepStatuses) {
        this.describeStepStatuses = describeStepStatuses;
    }

    public DescribeStepStatus addDescribeStep(DescribeStep describeStep) {
        CString key = CString.valueOf((char) describeStep.getCode() + ":").append(describeStep.getName());
        DescribeStepStatus describeStepStatus = new DescribeStepStatus(describeStep, null, null);
        // Retain internal buffers of describe step status
        describeStepStatus.retain();
        DescribeStepStatus previousDescribeStepStatus = getDescribeStepStatuses().put(key, describeStepStatus);
        if (previousDescribeStepStatus != null) {
            // Release internal buffers of describe step status
            previousDescribeStepStatus.release();
        }
        return describeStepStatus;
    }

    public void removeDescribeStep(byte code, CString name) {
        CString key = CString.valueOf((char) code + ":").append(name);
        // Remove describe step status
        DescribeStepStatus previousDescribeStepStatus = getDescribeStepStatuses().remove(key);
        if (previousDescribeStepStatus != null) {
            // Release internal buffers of describe step status
            previousDescribeStepStatus.release();
        }
    }

    public void resetDescribeSteps() {
        if (describeStepStatuses != null) {
            synchronized (describeStepStatuses) {
                for (DescribeStepStatus describeStepStatus : describeStepStatuses.values()) {
                    // Release internal buffers of describe step status
                    describeStepStatus.release();
                }
                describeStepStatuses.clear();
            }
        }
    }

    public DescribeStepStatus getDescribeStepStatus(byte code, CString name) {
        CString key = CString.valueOf((char) code + ":").append(name);
        return getDescribeStepStatuses().get(key);
    }

    public DescribeStepStatus getCurrentDescribeStepStatus() {
        if (currentDescribeStepKey != null) {
            return getDescribeStepStatuses().get(currentDescribeStepKey);
        }
        return null;
    }

    public void setCurrentDescribeStepStatus(DescribeStepStatus describeStepStatus) {
        this.currentDescribeStepKey = describeStepStatus != null
                ? CString.valueOf((char) describeStepStatus.getQuery().getCode() + ":")
                        .append(describeStepStatus.getQuery().getName())
                : null;
    }

    public SortedMap<String, CursorContext> getCursorContexts() {
        if (cursorContexts == null) {
            cursorContexts = Collections.synchronizedSortedMap(new TreeMap<>());
        }
        return cursorContexts;
    }

    public void setCursorContexts(SortedMap<String, CursorContext> cursorContexts) {
        this.cursorContexts = cursorContexts;
    }

    public CursorContext saveCursorContext(String name) {
        CursorContext cursorContext = new CursorContext(name, getPromise(), isResultProcessingEnabled(),
                isUnprotectingDataEnabled(), getCommandInvolvedBackends(), getExpectedFields());
        getCursorContexts().put(name, cursorContext);
        return cursorContext;
    }

    public void removeCursorContext(String name) {
        getCursorContexts().remove(name);
    }

    public CursorContext getCursorContext(String name) {
        return getCursorContexts().get(name);
    }

    public void restoreCursorContext(String name) {
        CursorContext cursorStatus = getCursorContext(name);
        if (cursorStatus != null) {
            setPromise(cursorStatus.getPromise());
            setResultProcessingEnabled(cursorStatus.isResultProcessingEnabled());
            setUnprotectingDataEnabled(cursorStatus.isUnprotectingDataEnabled());
            setCommandInvolvedBackends(cursorStatus.getInvolvedBackends());
            setExpectedFields(cursorStatus.getExpectedFields());
            setCurrentCommandOperation(Operation.READ);
        }
    }

    public void resetCursorContexts() {
        if (cursorContexts != null) {
            synchronized (cursorContexts) {
                cursorContexts.clear();
            }
        }
    }

    public void resetCurrentCommand() {
        setCurrentCommandOperation(null);
        setPromise(null);
        setResultProcessingEnabled(false);
        setExpectedFields(null);
        resetBackendRowDescriptions();
        setJoinFieldIndexes(null);
        resetRowDescription();
        setCurrentDescribeStepStatus(null);
        setCommandInvolvedBackends(null);
    }

    public void resetCurrentQuery() {
        setQueryInvolvedBackends(null);
        resetQueryResponses();
    }

    public void resetCurrentQueries() {
        resetCurrentQuery();
        resetBufferedQueries();
        resetQueryResponsesToIgnore();
    }

    public void reset() {
        setUser(null);
        setDatabaseName(null);
        setAuthenticationPhase(null);
        setTransactionStatus((byte) 'I');
        setInDatasetCreation(false);
        resetDatasetDefinition();
        setTransferMode(null);
        setTableDefinitionEnabled(false);
        resetCurrentCommand();
        resetCurrentQueries();
        resetCursorContexts();
    }

    public void waitForResponses() throws InterruptedException {
        if (responsesReceived == null || responsesReceived.getCount() == 0) {
            synchronized (this) {
                if (responsesReceived == null || responsesReceived.getCount() == 0) {
                    responsesReceived = new CountDownLatch(1);
                }
            }
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("waiting for responses");
        }
        // wait for responses
        responsesReceived.await();
        synchronized (this) {
            if (responsesReceived != null && responsesReceived.getCount() == 0) {
                responsesReceived = null;
            }
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("responses have been received");
        }
    }

    public void responsesReceived() {
        CountDownLatch responsesReceived;
        synchronized (this) {
            responsesReceived = this.responsesReceived;
        }
        if (responsesReceived != null && responsesReceived.getCount() == 1) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("notifying responses have been received");
            }
            // notify response is received
            responsesReceived.countDown();
        }
    }
}
