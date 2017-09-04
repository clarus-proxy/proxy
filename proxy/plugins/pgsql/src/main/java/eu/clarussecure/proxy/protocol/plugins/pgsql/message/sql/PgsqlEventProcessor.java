package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

import org.postgis.Geometry;
import org.postgis.PGboxbase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.dataoperations.DataOperationCommand;
import eu.clarussecure.proxy.protocol.plugins.pgsql.GeometryType;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConfiguration;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConstants;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlColumnsFinder;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage.Field;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession.CursorContext;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession.DescribeStepStatus;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession.ExpectedField;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession.ExpectedProtectedField;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession.ExtendedQueryStatus;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession.QueryResponseType;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data.Type;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data.TypeParser;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data.TypeWriter;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data.Types;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.InboundDataOperation;
import eu.clarussecure.proxy.spi.MetadataOperation;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.ModuleOperation;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.OutboundDataOperation;
import eu.clarussecure.proxy.spi.OutboundDataOperation.Criterion;
import eu.clarussecure.proxy.spi.StringUtilities;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCounted;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Not;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ArrayElement;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserConstants;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.Token;
import net.sf.jsqlparser.parser.TokenMgrError;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Database;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterExpression.ColumnDataType;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.close.CursorClose;
import net.sf.jsqlparser.statement.commit.Commit;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.declare.cursor.DeclareCursor;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.fetch.CursorFetch;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.start.StartTransaction;
import net.sf.jsqlparser.statement.update.Update;

public class PgsqlEventProcessor implements EventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);
    private static boolean FORCE_SQL_PROCESSING;
    static {
        String sqlForceProcessing = System.getProperty("pgsql.sql.force.processing", "false");
        FORCE_SQL_PROCESSING = Boolean.TRUE.toString().equalsIgnoreCase(sqlForceProcessing)
                || "1".equalsIgnoreCase(sqlForceProcessing) || "yes".equalsIgnoreCase(sqlForceProcessing)
                || "on".equalsIgnoreCase(sqlForceProcessing);
    }
    private static final boolean CHECK_BUFFER_REFERENCE_COUNT;
    static {
        String bufferCheckReferenceCount = System.getProperty("buffer.check.reference.count", "false");
        CHECK_BUFFER_REFERENCE_COUNT = Boolean.TRUE.toString().equalsIgnoreCase(bufferCheckReferenceCount)
                || "1".equalsIgnoreCase(bufferCheckReferenceCount) || "yes".equalsIgnoreCase(bufferCheckReferenceCount)
                || "on".equalsIgnoreCase(bufferCheckReferenceCount);
    }

    public static final String USER_KEY = "user";
    public static final String DATABASE_KEY = "database";

    private final static int AUTHENTICATION_OK = 0;
    private final static int AUTHENTICATION_CLEARTEXT_PASSWORD = 3;
    private final static int AUTHENTICATION_MD5_PASSWORD = 5;

    // significant functions
    public static final String FUNCTION_METADATA = "CLARUS_METADATA";
    public static final String FUNCTION_PROTECTED = "CLARUS_PROTECTED";
    public static final String FUNCTION_HAS_COLUMN_PRIVILEGE = "has_column_privilege";
    public static final String FUNCTION_ADD_GEOMETRY_COLUMN = "AddGeometryColumn";
    public static final String FUNCTION_ST_AS_BINARY = "ST_AsBinary";
    public static final String FUNCTION_ST_AS_TEXT = "ST_AsText";
    public static final String FUNCTION_ST_EXTENT = "ST_Extent";
    public static final String FUNCTION_ST_ESTIMATED_EXTENT = "ST_EstimatedExtent";
    public static final String FUNCTION_UPPER = "upper";

    public String[] FUNCTIONS_WITH_SUPPORTED_RETURN_TYPE = new String[] { FUNCTION_ST_AS_BINARY, FUNCTION_ST_AS_TEXT,
            FUNCTION_ST_EXTENT, FUNCTION_ST_ESTIMATED_EXTENT, FUNCTION_UPPER };

    // Pattern to test if a data identifier is formatted as 'database/schema.table/column'
    private static final Pattern FQ_DATA_ID_PATTERN = Pattern
            .compile("([\\w_\\*]+)/([\\w_\\*]+)\\.([\\w_\\*]+)/([\\w_\\*]+)");

    private static final String SQL_DATABASE_SCHEMA = "SQL_DATABASE_SCHEMA";

    @Override
    public MessageTransferMode<Map<CString, CString>, Void> processUserIdentification(ChannelHandlerContext ctx,
            Map<CString, CString> parameters) throws IOException {
        LOGGER.debug("User identification parameters: {}", parameters);
        TransferMode transferMode;
        Map<CString, CString> newParameters = parameters;
        List<Map<CString, CString>> allNewParameters = Collections.singletonList(newParameters);
        Map<Byte, CString> errorDetails = null;
        SQLSession session = getSession(ctx);
        CString databaseName = parameters.get(CString.valueOf(DATABASE_KEY));
        session.setDatabaseName(databaseName.toString());
        List<CString> databaseNames = Collections.singletonList(databaseName);
        List<CString> newDatabaseNames = databaseNames;
        List<String> backendDatabaseNames = getBackendDatabaseNames(ctx);
        boolean sameDatabaseNames = backendDatabaseNames.stream().distinct().count() <= 1;
        if (!sameDatabaseNames) {
            newDatabaseNames = backendDatabaseNames.stream().map(CString::valueOf).collect(Collectors.toList());
        }
        CString newDatabaseName = newDatabaseNames.get(0);
        CString userName = parameters.get(CString.valueOf(USER_KEY));
        CString newUserName = getProtocolService(ctx).newUserIdentification(userName);
        if (newUserName != null) {
            int numberOfBackends = getNumberOfBackends(ctx);
            if (!newUserName.equals(userName) || !newDatabaseNames.equals(databaseNames)) {
                if (sameDatabaseNames) {
                    newParameters = new HashMap<CString, CString>(parameters);
                    newParameters.forEach((k, v) -> {
                        k.retain();
                        if ((!k.equals(CString.valueOf("user")) || newUserName.equals(userName))
                                && (!k.equals(CString.valueOf("database")) || newDatabaseName.equals(databaseName))) {
                            v.retain();
                        }
                    });
                    if (!newUserName.equals(userName)) {
                        newParameters.put(CString.valueOf("user"), newUserName);
                    }
                    if (!newDatabaseName.equals(databaseName)) {
                        newParameters.put(CString.valueOf("database"), newDatabaseName);
                    }
                } else {
                    allNewParameters = new ArrayList<>(numberOfBackends);
                    for (int i = 0; i < numberOfBackends; i++) {
                        CString backendNewDatabaseName = newDatabaseNames.get(i);
                        newParameters = new HashMap<CString, CString>(parameters);
                        newParameters.forEach((k, v) -> {
                            k.retain();
                            if ((!k.equals(CString.valueOf("user")) || newUserName.equals(userName))
                                    && (!k.equals(CString.valueOf("database"))
                                            || backendNewDatabaseName.equals(databaseName))) {
                                v.retain();
                            }
                        });
                        if (!newUserName.equals(userName)) {
                            newParameters.put(CString.valueOf("user"), newUserName);
                        }
                        if (!backendNewDatabaseName.equals(databaseName)) {
                            newParameters.put(CString.valueOf("database"), backendNewDatabaseName);
                        }
                        allNewParameters.add(newParameters);
                    }
                }
            }
            transferMode = TransferMode.FORWARD;
            // Add user to session.
            session.setUser(userName.toString());
            // Prepare authentications
            SQLSession.AuthenticationPhase authenticationPhase = new SQLSession.AuthenticationPhase(numberOfBackends);
            session.setAuthenticationPhase(authenticationPhase);
            session.setQueryInvolvedBackends(IntStream.range(0, numberOfBackends).boxed().collect(Collectors.toList()));
        } else {
            transferMode = TransferMode.ERROR;
            errorDetails = new LinkedHashMap<>();
            errorDetails.put((byte) 'S', CString.valueOf("FATAL"));
            errorDetails.put((byte) 'C', CString.valueOf("28000"));
            errorDetails.put((byte) 'M', CString.valueOf("Access denied"));
        }

        MessageTransferMode<Map<CString, CString>, Void> mode;
        if (sameDatabaseNames) {
            mode = new MessageTransferMode<>(transferMode, newParameters, null, errorDetails);
        } else {
            mode = new MessageTransferMode<>(transferMode, allNewParameters, null, errorDetails);
        }
        LOGGER.debug("User identification processed: transfer mode={}, new parameters={}, error={}",
                mode.getTransferMode(), mode.getNewContent(), mode.getErrorDetails());
        return mode;
    }

    @Override
    public MessageTransferMode<AuthenticationResponse, Void> processAuthenticationResponse(ChannelHandlerContext ctx,
            AuthenticationResponse response) throws IOException {
        LOGGER.debug("Authentication response type: {}", response.getType());
        TransferMode transferMode;
        AuthenticationResponse newResponse = response;
        SQLSession session = getSession(ctx);

        // Forget all responses except the one from the preferred backend
        int backend = getBackend(ctx);
        int preferredBackend = getPreferredBackend(ctx);
        if (backend != preferredBackend) {
            transferMode = TransferMode.FORGET;
            newResponse = null;
        } else {
            transferMode = TransferMode.FORWARD;
            // Case where authentication type is MD5, modify type to Cleartext
            int type = response.getType();
            int newType = AUTHENTICATION_MD5_PASSWORD == type ? AUTHENTICATION_CLEARTEXT_PASSWORD : type;
            if (newType != type) {
                newResponse = new AuthenticationResponse(newType, null);
            }
        }

        // Save authentication type and parameters to session
        SQLSession.AuthenticationPhase authenticationPhase = session.getAuthenticationPhase();
        boolean allResponsesReceived = authenticationPhase.setAndCountAuthenticationResponse(backend, response);
        if (allResponsesReceived) {
            // Case where authentication type is OK, reset authentications
            if (AUTHENTICATION_OK == response.getType()) {
                session.setAuthenticationPhase(null);
            } else {
                // notify other thread authentication parameters are all
                // received
                authenticationPhase.allResponsesReceived();
            }
        }

        MessageTransferMode<AuthenticationResponse, Void> mode = new MessageTransferMode<>(transferMode, newResponse);
        LOGGER.debug("Authentication response processed: transfer mode={}, new type={}", mode.getTransferMode(),
                mode.getNewContent());
        return mode;
    }

    @Override
    public MessageTransferMode<CString, Void> processUserAuthentication(ChannelHandlerContext ctx, CString password)
            throws IOException {
        LOGGER.debug("User authentication pawword: {}", password);
        TransferMode transferMode;
        List<CString> newPasswords = Collections.singletonList(password);
        Map<Byte, CString> errorDetails = null;
        SQLSession session = getSession(ctx);
        String userName = session.getUser();

        CString[] userCredentials = getProtocolService(ctx).userAuthentication(CString.valueOf(userName), password);
        if (userCredentials != null) {
            transferMode = TransferMode.FORWARD;
            CString newPassword = userCredentials[1];
            SQLSession.AuthenticationPhase authenticationPhase = session.getAuthenticationPhase();
            // Wait all authentication parameters
            authenticationPhase.waitForAllResponses();
            int nbBackends = authenticationPhase.getNbAuthenticationResponses();
            newPasswords = new ArrayList<>(nbBackends);
            for (int i = 0; i < nbBackends; i++) {
                AuthenticationResponse authenticationResponse = authenticationPhase.getAuthenticationResponse(i);
                // Handle case where MD5 password is needed
                if (AUTHENTICATION_MD5_PASSWORD == authenticationResponse.getType()) {
                    ByteBuf parameters = authenticationResponse.getParameters();
                    // Retrieve salt from authentication parameters
                    byte[] salt = new byte[parameters.readableBytes()];
                    parameters.readBytes(salt);

                    // Create MD5 digester
                    MessageDigest digest;
                    try {
                        digest = MessageDigest.getInstance("MD5");
                    } catch (NoSuchAlgorithmException e) {
                        throw new IOException(e);
                    }

                    // Generate MD5 hash for password + user name
                    digest.update((newPassword.toString() + userName).getBytes());
                    String pwdUsrEnc = DatatypeConverter.printHexBinary(digest.digest()).toLowerCase();

                    // Generate MD5 hash for hashed password + user name & salt
                    // Finally, add "md5" at the newly hashed password
                    digest.update(pwdUsrEnc.getBytes());
                    digest.update(salt);
                    String passwordEncrypted = "md5" + DatatypeConverter.printHexBinary(digest.digest()).toLowerCase();

                    // MD5 password is equivalent to SQL concat('md5',
                    // md5(concat(md5(concat(password, username)),
                    // random-salt)))
                    newPasswords.add(CString.valueOf(passwordEncrypted));
                } else {
                    newPasswords.add(newPassword);
                }
                authenticationPhase.setAuthenticationResponse(i, null);
            }
        } else {
            transferMode = TransferMode.ERROR;
            newPasswords = null;
            errorDetails = new LinkedHashMap<>();
            errorDetails.put((byte) 'S', CString.valueOf("FATAL"));
            errorDetails.put((byte) 'C', CString.valueOf("28P01"));
            errorDetails.put((byte) 'M',
                    CString.valueOf(String.format("password authentication failed for user \"%s\"", userName)));
        }

        MessageTransferMode<CString, Void> mode = new MessageTransferMode<>(transferMode, newPasswords, null,
                errorDetails);
        LOGGER.debug("User authentication processed: transfer mode={}, new password(s)=<XXX>, error={}",
                mode.getTransferMode(), mode.getErrorDetails());
        return mode;
    }

    @Override
    public QueriesTransferMode<SQLStatement, CommandResults> processStatement(ChannelHandlerContext ctx,
            SQLStatement sqlStatement) throws IOException {
        LOGGER.debug("SQL statement: {}", sqlStatement);
        TransferMode transferMode = TransferMode.FORWARD;
        Map<Integer, List<Query>> newQueries = Collections.singletonMap(0, Collections.singletonList(sqlStatement));
        CommandResults response = null;
        Map<Byte, CString> errorDetails = null;
        boolean toProcess = false;
        Operation operation = null;
        SQLSession session = getSession(ctx);
        session.setCurrentCommandOperation(operation);
        SQLCommandType type = SimpleSQLParserUtil.parse(sqlStatement.getSQL());
        if (type != null) {
            CString completeTag = null;
            CString error = null;
            switch (type) {
            case CLARUS_METADATA:
            case CLARUS_PROTECTED: {
                if (session.isProcessingQuery()) {
                    transferMode = TransferMode.ERROR;
                    error = CString.valueOf(String.format("%s is not supported while processing one or several queries",
                            type.getPattern()));
                } else {
                    toProcess = true;
                    operation = Operation.READ;
                    session.setCurrentCommandOperation(operation);
                }
                break;
            }
            case SET:
            case FETCH_CURSOR:
            case CLOSE_CURSOR:
                toProcess = isSQLStatementToProcess(null);
                break;
            case START_TRANSACTION: {
                toProcess = isSQLStatementToProcess(null);
                if (sqlStatement instanceof SimpleQuery && session.getTransactionStatus() != (byte) 'E') {
                    // transaction status is followed by tracking ready for
                    // query responses
                    // however, in case of simple query, starting a transaction
                    // may be part of a whole script in a single query
                    // in that case, we can't wait for ready for query response
                    // so we suppose starting a transaction is ok (provided that
                    // transaction status is not error)
                    session.setTransactionStatus((byte) 'T');
                }
                break;
            }
            case COMMIT:
            case ROLLBACK: {
                toProcess = isSQLStatementToProcess(null);
                if (sqlStatement instanceof SimpleQuery && session.getTransactionStatus() != (byte) 'E') {
                    // transaction status is followed by tracking ready for
                    // query responses
                    // however, in case of simple query, closing a transaction
                    // may be part of a whole script in a single query
                    // in that case, we can't wait for ready for query response
                    // so we suppose closing a transaction is ok (provided that
                    // transaction status is not error)
                    session.setTransactionStatus((byte) 'I');
                }
                if (session.isInDatasetCreation()) {
                    // Closing a transaction completes creation of a dataset
                    // (arbitrary decision)
                    session.setInDatasetCreation(false);
                }
                break;
            }
            case SELECT:
            case DECLARE_CURSOR: {
                operation = Operation.READ;
                // TODO detect if select is on the whole dataset
                boolean retrieveWholeDataset = false;
                Mode processingMode = getProcessingMode(ctx, retrieveWholeDataset, operation);
                toProcess = isSQLStatementToProcess(processingMode);
                if (toProcess) {
                    if (processingMode == null) {
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(String.format("%s read not supported by this CLARUS proxy",
                                retrieveWholeDataset ? "Dataset" : "Record"));
                    } else if (processingMode == Mode.ORCHESTRATION) {
                        // TODO orchestration mode. Meanwhile, same as AS_IT_IS
                        // mode
                        session.setCurrentCommandOperation(operation);
                    } else if (processingMode == Mode.AS_IT_IS || processingMode == Mode.BUFFERING
                            || processingMode == Mode.STREAMING) {
                        session.setCurrentCommandOperation(operation);
                    }
                }
                break;
            }
            case INSERT: {
                operation = Operation.CREATE;
                boolean inDatasetCreation = session.isInDatasetCreation();
                Mode processingMode = getProcessingMode(ctx, inDatasetCreation, operation);
                toProcess = isSQLStatementToProcess(processingMode);
                if (toProcess) {
                    if (processingMode == null) {
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(String.format("%s creation not supported by this CLARUS proxy",
                                inDatasetCreation ? "Dataset" : "Record"));
                    } else if (processingMode == Mode.BUFFERING) {
                        if (inDatasetCreation) {
                            transferMode = TransferMode.FORGET;
                            if (sqlStatement instanceof SimpleQuery) {
                                completeTag = CString.valueOf("INSERT 0 1");
                            }
                        } else {
                            // Should not occur
                            transferMode = TransferMode.ERROR;
                            error = CString.valueOf(
                                    "Buffering processing mode not supported for record creation by this CLARUS proxy");
                        }
                    } else if (processingMode == Mode.ORCHESTRATION) {
                        // Should not occur
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(
                                "Orchestration processing mode not supported for dataset or record creation by this CLARUS proxy");
                    } else if (processingMode == Mode.AS_IT_IS || processingMode == Mode.STREAMING) {
                        session.setCurrentCommandOperation(operation);
                    }
                }
                break;
            }
            case CREATE_TABLE: {
                operation = Operation.CREATE;
                Mode processingMode = getProcessingMode(ctx, true, operation);
                toProcess = isSQLStatementToProcess(processingMode);
                if (toProcess) {
                    if (processingMode == null) {
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf("Dataset creation not supported by this CLARUS proxy");
                    } else if (processingMode == Mode.BUFFERING) {
                        if (session.getTransactionStatus() == (byte) 'T') {
                            session.setInDatasetCreation(true);
                        }
                        transferMode = TransferMode.FORGET;
                        if (sqlStatement instanceof SimpleQuery) {
                            completeTag = CString.valueOf("CREATE TABLE");
                        }
                    } else if (processingMode == Mode.ORCHESTRATION) {
                        // Should not occur
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(
                                "Orchestration processing mode not supported for dataset creation by this CLARUS proxy");
                    } else if (processingMode == Mode.AS_IT_IS || processingMode == Mode.STREAMING) {
                        if (session.getTransactionStatus() == (byte) 'T') {
                            session.setInDatasetCreation(true);
                        }
                        session.setCurrentCommandOperation(operation);
                    }
                }
                break;
            }
            case ALTER_TABLE:
            case ADD_GEOMETRY_COLUMN:
            case CREATE_INDEX: {
                boolean inDatasetCreation = session.isInDatasetCreation();
                operation = inDatasetCreation ? Operation.CREATE : Operation.UPDATE;
                Mode processingMode = getProcessingMode(ctx, session.getTransactionStatus() == (byte) 'T', operation);
                toProcess = isSQLStatementToProcess(processingMode);
                if (toProcess) {
                    if (processingMode == null) {
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(String.format("Dataset %s not supported by this CLARUS proxy",
                                inDatasetCreation ? "creation" : "structure modification"));
                    } else if (processingMode == Mode.BUFFERING) {
                        if (inDatasetCreation) {
                            transferMode = TransferMode.FORGET;
                            if (sqlStatement instanceof SimpleQuery) {
                                switch (type) {
                                case ADD_GEOMETRY_COLUMN:
                                    completeTag = CString.valueOf("SELECT 1");
                                    break;
                                default:
                                    completeTag = CString.valueOf(type.getPattern());
                                }
                            }
                        } else {
                            // Should not occur
                            transferMode = TransferMode.ERROR;
                            error = CString.valueOf(
                                    "Buffering processing mode not supported for dataset structure modification by this CLARUS proxy");
                        }
                    } else if (processingMode == Mode.ORCHESTRATION) {
                        // Should not occur
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(
                                "Orchestration processing mode not supported for dataset or record creation by this CLARUS proxy");
                    } else if (processingMode == Mode.AS_IT_IS || processingMode == Mode.STREAMING) {
                        session.setCurrentCommandOperation(operation);
                    }
                }
                break;
            }
            case DROP_TABLE: {
                operation = Operation.DELETE;
                Mode processingMode = getProcessingMode(ctx, true, operation);
                toProcess = isSQLStatementToProcess(processingMode);
                if (toProcess) {
                    if (processingMode == null) {
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf("Dataset delete not supported by this CLARUS proxy");
                    } else if (processingMode == Mode.BUFFERING) {
                        transferMode = TransferMode.FORGET;
                        if (sqlStatement instanceof SimpleQuery) {
                            completeTag = CString.valueOf("DROP TABLE");
                        }
                    } else if (processingMode == Mode.ORCHESTRATION) {
                        // Should not occur
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(
                                "Orchestration processing mode not supported for dataset delete by this CLARUS proxy");
                    } else if (processingMode == Mode.AS_IT_IS || processingMode == Mode.STREAMING) {
                        session.setCurrentCommandOperation(operation);
                    }
                }
                break;
            }
            case UPDATE: {
                operation = Operation.UPDATE;
                // TODO detect if update is on the whole dataset
                boolean inDatasetModification = false;
                Mode processingMode = getProcessingMode(ctx, inDatasetModification, operation);
                toProcess = isSQLStatementToProcess(processingMode);
                if (toProcess) {
                    if (processingMode == null) {
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(String.format("%s update not supported by this CLARUS proxy",
                                inDatasetModification ? "Dataset" : "Record"));
                    } else if (processingMode == Mode.BUFFERING) {
                        if (inDatasetModification) {
                            transferMode = TransferMode.FORGET;
                            if (sqlStatement instanceof SimpleQuery) {
                                completeTag = CString.valueOf("UPDATE 0 1");
                            }
                        } else {
                            // Should not occur
                            transferMode = TransferMode.ERROR;
                            error = CString.valueOf(
                                    "Buffering processing mode not supported for record modification by this CLARUS proxy");
                        }
                    } else if (processingMode == Mode.ORCHESTRATION) {
                        // Should not occur
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(
                                "Orchestration processing mode not supported for dataset or record modification by this CLARUS proxy");
                    } else if (processingMode == Mode.AS_IT_IS || processingMode == Mode.STREAMING) {
                        session.setCurrentCommandOperation(operation);
                    }
                }
                break;
            }
            case DELETE: {
                operation = Operation.DELETE;
                // TODO detect if delete is on the whole dataset
                boolean deleteWholeDataset = false;
                Mode processingMode = getProcessingMode(ctx, deleteWholeDataset, operation);
                toProcess = isSQLStatementToProcess(processingMode);
                if (toProcess) {
                    if (processingMode == null) {
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(String.format("%s delete not supported by this CLARUS proxy",
                                deleteWholeDataset ? "Dataset" : "Record"));
                    } else if (processingMode == Mode.BUFFERING) {
                        if (deleteWholeDataset) {
                            transferMode = TransferMode.FORGET;
                            if (sqlStatement instanceof SimpleQuery) {
                                completeTag = CString.valueOf("DELETE 0 1");
                            }
                        } else {
                            // Should not occur
                            transferMode = TransferMode.ERROR;
                            error = CString.valueOf(
                                    "Buffering processing mode not supported for record delete by this CLARUS proxy");
                        }
                    } else if (processingMode == Mode.ORCHESTRATION) {
                        // Should not occur
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(
                                "Orchestration processing mode not supported for dataset or record delete by this CLARUS proxy");
                    } else {
                        session.setCurrentCommandOperation(operation);
                    }
                }
                break;
            }
            default:
                break;
            }
            if (completeTag != null) {
                response = new CommandResults(completeTag);
            }
            if (error != null) {
                errorDetails = new LinkedHashMap<>();
                errorDetails.put((byte) 'S', CString.valueOf("FATAL"));
                errorDetails.put((byte) 'M', error);
            }
        }
        if (transferMode == TransferMode.FORWARD) {
            if (sqlStatement instanceof SimpleQuery) {
                // Set involved backends
                session.setCommandInvolvedBackends(Collections.singletonList(getPreferredBackend(ctx)), false);
                // Process simple query immediately
                Result<List<Query>, CommandResults, CString> result = buildNewQueries(ctx,
                        (SimpleSQLStatement) sqlStatement, operation, toProcess);
                if (result.isQuery()) {
                    // Retrieve modified (or unmodified) queries to forward
                    newQueries = result.queriesAsMap();
                } else if (result.isResponse()) {
                    // Forget the query and reply immediately
                    transferMode = TransferMode.FORGET;
                    newQueries = null;
                    response = result.response();
                } else if (result.isError()) {
                    transferMode = TransferMode.ERROR;
                    errorDetails = new LinkedHashMap<>();
                    errorDetails.put((byte) 'S', CString.valueOf("FATAL"));
                    errorDetails.put((byte) 'M', result.error());
                    if (session.getTransactionStatus() == (byte) 'T') {
                        session.setTransactionStatus((byte) 'E');
                        session.setTransactionErrorDetails(errorDetails);
                    }
                    session.resetCurrentCommand();
                    newQueries = null;
                }
            } else {
                // Track parse step (for bind, describe and close steps)
                session.addParseStep((ParseStep) sqlStatement, operation, type, toProcess,
                        Collections.singletonList(getPreferredBackend(ctx)));
                // Postpone processing of extended query
                transferMode = TransferMode.FORGET;
                newQueries = null;
                response = new CommandResults();
                response.setParseCompleteRequired(true);
            }
        } else if (transferMode == TransferMode.FORGET) {
            if (sqlStatement instanceof SimpleQuery) {
                // Set involved backends
                session.setCommandInvolvedBackends(Collections.singletonList(getPreferredBackend(ctx)), false);
                // Buffer sql statement
                errorDetails = bufferQuery(ctx, sqlStatement);
                if (errorDetails != null) {
                    transferMode = TransferMode.ERROR;
                    session.resetCurrentCommand();
                }
            } else {
                // Track parse step (for bind, describe and close steps)
                session.addParseStep((ParseStep) sqlStatement, operation, type, toProcess,
                        Collections.singletonList(getPreferredBackend(ctx)));
                response = new CommandResults();
                response.setParseCompleteRequired(true);
            }
            newQueries = null;
        } else if (transferMode == TransferMode.ERROR) {
            if (session.getTransactionStatus() == (byte) 'T') {
                session.setTransactionStatus((byte) 'E');
                session.setTransactionErrorDetails(errorDetails);
            }
            session.resetCurrentCommand();
            newQueries = null;
        }
        session.setTransferMode(transferMode);
        QueriesTransferMode<SQLStatement, CommandResults> mode = new QueriesTransferMode<>(transferMode, newQueries,
                response, errorDetails);
        LOGGER.debug("SQL statement processed: new queries={}, transfer mode={}", mode.getNewQueries(),
                mode.getTransferMode());
        return mode;
    }

    private boolean isSQLStatementToProcess(Mode processingMode) {
        return FORCE_SQL_PROCESSING || processingMode != Mode.AS_IT_IS;
    }

    private Result<List<Query>, CommandResults, CString> buildNewQueries(ChannelHandlerContext ctx,
            SimpleSQLStatement sqlStatement, Operation operation, boolean toProcess) throws IOException {
        SQLSession session = getSession(ctx);
        List<SimpleSQLStatement> newSQLStatements = null;
        if (toProcess) {
            Result<SimpleSQLStatement, CommandResults, CString> result = processSimpleSQLStatement(ctx, sqlStatement,
                    operation);
            if (result.isQuery()) {
                newSQLStatements = result.queries();
            } else if (result.isResponse()) {
                return Result.response(result.response());
            } else if (result.isError()) {
                return Result.error(result.error());
            }
        } else {
            newSQLStatements = Collections.singletonList(sqlStatement);
        }
        List<List<Query>> bufferedQueries = processBufferedQueries(ctx);
        List<List<Query>> newQueries = null;
        if (bufferedQueries.isEmpty() || bufferedQueries.stream().allMatch(List::isEmpty)) {
            if (newSQLStatements != null) {
                newQueries = newSQLStatements.stream()
                        .map(ns -> ns != null ? Collections.<Query>singletonList(ns) : null)
                        .collect(Collectors.toList());
            }
        } else {
            int nbDirected = bufferedQueries.size() > newSQLStatements.size() ? bufferedQueries.size()
                    : newSQLStatements.size();
            // Compute new SQL statement size (if buffered queries are all
            // simple)
            boolean allSimple = true;
            int newSize = 0;
            for (int i = 0; i < nbDirected; i++) {
                List<? extends Query> directedBufferedQueries = i < bufferedQueries.size() ? bufferedQueries.get(i)
                        : null;
                for (Query bufferedQuery : directedBufferedQueries) {
                    if (bufferedQuery instanceof SimpleQuery) {
                        newSize += ((SQLStatement) bufferedQuery).getSQL().length();
                    } else {
                        allSimple = false;
                        break;
                    }
                }
            }
            newQueries = new ArrayList<>(nbDirected);
            for (int i = 0; i < nbDirected; i++) {
                List<? extends Query> directedBufferedQueries = i < bufferedQueries.size() ? bufferedQueries.get(i)
                        : null;
                SimpleSQLStatement directedSQLStatement = i < newSQLStatements.size() ? newSQLStatements.get(i) : null;
                List<Query> directedQueries;
                if (directedBufferedQueries == null || directedBufferedQueries.isEmpty()) {
                    if (directedSQLStatement != null) {
                        directedQueries = Collections.singletonList(directedSQLStatement);
                    } else {
                        directedQueries = Collections.emptyList();
                    }
                } else {
                    if (allSimple) {
                        if (directedSQLStatement != null) {
                            newSize += directedSQLStatement.getSQL().length();
                        }
                        CString newSQL = CString.valueOf(new StringBuilder(newSize));
                        for (Query bufferedQuery : directedBufferedQueries) {
                            newSQL.append(((SQLStatement) bufferedQuery).getSQL());
                        }
                        if (directedSQLStatement != null) {
                            newSQL.append(directedSQLStatement.getSQL());
                        }
                        SimpleSQLStatement newDirectedSQLStatement = new SimpleSQLStatement(newSQL);
                        directedQueries = Collections.singletonList(newDirectedSQLStatement);
                    } else {
                        directedQueries = new ArrayList<>(directedBufferedQueries);
                        if (directedSQLStatement != null) {
                            directedQueries.add(directedSQLStatement);
                        }
                    }
                }
                newQueries.add(directedQueries);
            }
            if (allSimple) {
                // Remove expected response messages
                QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
                while (nextQueryResponseToIgnore == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                    session.removeFirstQueryResponseToIgnore();
                    nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
                }
            }
        }
        return Result.queries(newQueries);
    }

    private List<List<Query>> processBufferedQueries(ChannelHandlerContext ctx) throws IOException {
        SQLSession session = getSession(ctx);
        List<Statement> referencedStatements = new ArrayList<>(session.getBufferedQueries().size());
        List<List<ParameterValue>> referencedParameterValues = new ArrayList<>(session.getBufferedQueries().size());
        List<OutboundDataOperation> referencedOutboundDataOperations = new ArrayList<>(
                session.getBufferedQueries().size());
        List<Integer> referencedRows = new ArrayList<>(session.getBufferedQueries().size());
        // Track indexes of parse steps (necessary for bind, describe and close
        // steps)
        Map<CString, Integer> lastParseStepIndexes = new HashMap<>();
        // Track indexes of bind steps (necessary for describe, execute and
        // close steps)
        Map<CString, Integer> lastBindStepIndexes = new HashMap<>();
        // Track usage of parse steps and bind steps to know if they must be
        // closed
        Map<CString, Integer> parseStepCounters = session.getParseStepStatuses().keySet().stream()
                // Initialize parse step counters
                .collect(Collectors.toMap(java.util.function.Function.identity(), i -> 1));
        Map<CString, Integer> bindStepCounters = session.getBindStepStatuses().keySet().stream()
                // Initialize bind step counters
                .collect(Collectors.toMap(java.util.function.Function.identity(), i -> 1));
        // Track outbound data operations
        OutboundDataOperation lastOutboundDataOperation = null;
        int queryIndex = 0;
        int row = 0;
        for (Query bufferedQuery : session.getBufferedQueries()) {
            int refIndex = -1;
            Statement refStmt = null;
            List<ParameterValue> refParamVals = null;
            Integer refRow = null;
            boolean extract = false;
            if (bufferedQuery instanceof SimpleQuery) {
                // Parse SQL statement
                refStmt = parseSQL(ctx, ((SimpleSQLStatement) bufferedQuery).getSQL());
                if (refStmt instanceof Insert) {
                    // Save row for this SQL statement
                    refRow = Integer.valueOf(row);
                    ItemsList values = ((Insert) refStmt).getItemsList();
                    row += values instanceof ExpressionList ? 1
                            : values instanceof MultiExpressionList
                                    ? ((MultiExpressionList) values).getExprList().size() : 1;
                }
                // Ready to extract and protect data for this query
                extract = true;
            } else {
                ExtendedQuery query = (ExtendedQuery) bufferedQuery;
                if (query instanceof ParseStep) {
                    ParseStep parseStep = (ParseStep) query;
                    // Parse SQL statement
                    refStmt = parseSQL(ctx, parseStep.getSQL());
                    if (refStmt instanceof Insert) {
                        // Save row for this SQL statement
                        // Note: row incremented during bind step
                        refRow = Integer.valueOf(row);
                    }
                    // Track index of this parse step
                    lastParseStepIndexes.put(parseStep.getName(), queryIndex);
                    // Increment parse step counter
                    Integer counter = parseStepCounters.get(parseStep.getName());
                    parseStepCounters.put(parseStep.getName(), counter + 1);
                } else if (query instanceof BindStep) {
                    // Extract parameter values
                    BindStep bindStep = (BindStep) query;
                    ParseStep parseStep = null;
                    if (lastParseStepIndexes.containsKey(bindStep.getPreparedStatement())) {
                        // Parse step was within buffered queries
                        refIndex = lastParseStepIndexes.get(bindStep.getPreparedStatement());
                        // Retrieve parse step from buffered queries
                        parseStep = (ParseStep) session.getBufferedQueries().get(refIndex);
                        // SQL statement was already parsed
                        refStmt = referencedStatements.get(refIndex);
                    } else {
                        // Parse step was done long time ago (and is tracked by
                        // session)
                        ExtendedQueryStatus<ParseStep> parseStepStatus = session
                                .getParseStepStatus(bindStep.getPreparedStatement());
                        if (parseStepStatus != null) {
                            parseStep = parseStepStatus.getQuery();
                            // Parse SQL statement
                            refStmt = parseSQL(ctx, parseStep.getSQL());
                        }
                    }
                    if (parseStep != null) {
                        // Build ParameterValue (type+format+value)
                        refParamVals = bindStep.getParameterValues().stream()
                                // transform each parameter to a ParameterValue
                                .map(ParameterValue::new)
                                // save as a list
                                .collect(Collectors.toList());
                        final List<Long> parameterTypes = parseStep.getParameterTypes();
                        for (int idx = 0; idx < parameterTypes.size(); idx++) {
                            // set parameter type
                            refParamVals.get(idx).setType(parameterTypes.get(idx));
                        }
                        final List<Short> formats = bindStep.getParameterFormats();
                        for (int i = 0; i < refParamVals.size(); i++) {
                            ParameterValue parameterValue = refParamVals.get(i);
                            Short format = formats.get(formats.size() == 1 ? 0 : i);
                            // set parameter value format
                            parameterValue.setFormat(format);
                        }
                        if (refStmt instanceof Insert) {
                            // Save row for this bind step
                            refRow = Integer.valueOf(row);
                            ItemsList values = ((Insert) refStmt).getItemsList();
                            row += values instanceof ExpressionList ? 1
                                    : values instanceof MultiExpressionList
                                            ? ((MultiExpressionList) values).getExprList().size() : 1;
                        }
                        // Ready to extract and protect data for this query
                        extract = true;
                    }
                    // Track index of this bind step
                    lastBindStepIndexes.put(bindStep.getName(), queryIndex);
                    // Increment bind step counter
                    Integer counter = bindStepCounters.get(bindStep.getName());
                    bindStepCounters.put(bindStep.getName(), counter + 1);
                } else if (query instanceof DescribeStep) {
                    DescribeStep describeStep = (DescribeStep) query;
                    if (describeStep.getCode() == 'S') {
                        if (lastParseStepIndexes.containsKey(describeStep.getName())) {
                            // Parse step was within buffered queries
                            refIndex = lastParseStepIndexes.get(describeStep.getName());
                            // Operation was already built
                            lastOutboundDataOperation = referencedOutboundDataOperations.get(refIndex);
                        } else {
                            // Parse step was done long time ago (and is tracked
                            // by
                            // session)
                            ExtendedQueryStatus<ParseStep> parseStepStatus = session
                                    .getParseStepStatus(describeStep.getName());
                            if (parseStepStatus != null) {
                                ParseStep parseStep = parseStepStatus.getQuery();
                                // Parse SQL statement
                                refStmt = parseSQL(ctx, parseStep.getSQL());
                                extract = true;
                            }
                        }
                    } else {
                        BindStep bindStep = null;
                        if (lastBindStepIndexes.containsKey(describeStep.getName())) {
                            // Bind step was within buffered queries
                            refIndex = lastBindStepIndexes.get(describeStep.getName());
                            // Operation was already built
                            lastOutboundDataOperation = referencedOutboundDataOperations.get(refIndex);
                        } else {
                            // Bind step was done long time ago (and is tracked
                            // by
                            // session)
                            ExtendedQueryStatus<BindStep> bindStepStatus = session
                                    .getBindStepStatus(describeStep.getName());
                            if (bindStepStatus != null) {
                                bindStep = bindStepStatus.getQuery();
                                ExtendedQueryStatus<ParseStep> parseStepStatus = session
                                        .getParseStepStatus(bindStep.getName());
                                if (parseStepStatus != null) {
                                    ParseStep parseStep = parseStepStatus.getQuery();
                                    // Parse SQL statement
                                    refStmt = parseSQL(ctx, parseStep.getSQL());
                                    // Build ParameterValue (type+format+value)
                                    refParamVals = bindStep.getParameterValues().stream()
                                            // transform each parameter to a
                                            // ParameterValue
                                            .map(ParameterValue::new)
                                            // save as a list
                                            .collect(Collectors.toList());
                                    final List<Long> parameterTypes = parseStep.getParameterTypes();
                                    for (int idx = 0; idx < parameterTypes.size(); idx++) {
                                        // set parameter type
                                        refParamVals.get(idx).setType(parameterTypes.get(idx));
                                    }
                                    final List<Short> formats = bindStep.getParameterFormats();
                                    for (int i = 0; i < refParamVals.size(); i++) {
                                        ParameterValue parameterValue = refParamVals.get(i);
                                        Short format = formats.get(formats.size() == 1 ? 0 : i);
                                        // set parameter value format
                                        parameterValue.setFormat(format);
                                    }
                                    // Ready to extract and protect data for
                                    // this query
                                    extract = true;
                                }
                            }
                        }
                    }
                } else if (query instanceof ExecuteStep) {
                    ExecuteStep executeStep = (ExecuteStep) query;
                    BindStep bindStep = null;
                    if (lastBindStepIndexes.containsKey(executeStep.getPortal())) {
                        // Bind step was within buffered queries
                        refIndex = lastBindStepIndexes.get(executeStep.getPortal());
                        // Operation was already built
                        lastOutboundDataOperation = referencedOutboundDataOperations.get(refIndex);
                    } else {
                        // Bind step was done long time ago (and is tracked by
                        // session)
                        ExtendedQueryStatus<BindStep> bindStepStatus = session
                                .getBindStepStatus(executeStep.getPortal());
                        if (bindStepStatus != null) {
                            bindStep = bindStepStatus.getQuery();
                            ExtendedQueryStatus<ParseStep> parseStepStatus = session
                                    .getParseStepStatus(bindStep.getName());
                            if (parseStepStatus != null) {
                                ParseStep parseStep = parseStepStatus.getQuery();
                                // Parse SQL statement
                                refStmt = parseSQL(ctx, parseStep.getSQL());
                                // Build ParameterValue (type+format+value)
                                refParamVals = bindStep.getParameterValues().stream()
                                        // transform each parameter to a
                                        // ParameterValue
                                        .map(ParameterValue::new)
                                        // save as a list
                                        .collect(Collectors.toList());
                                final List<Long> parameterTypes = parseStep.getParameterTypes();
                                for (int idx = 0; idx < parameterTypes.size(); idx++) {
                                    // set parameter type
                                    refParamVals.get(idx).setType(parameterTypes.get(idx));
                                }
                                final List<Short> formats = bindStep.getParameterFormats();
                                for (int i = 0; i < refParamVals.size(); i++) {
                                    ParameterValue parameterValue = refParamVals.get(i);
                                    Short format = formats.get(formats.size() == 1 ? 0 : i);
                                    // set parameter value format
                                    parameterValue.setFormat(format);
                                }
                                // Ready to extract and protect data for this
                                // query
                                extract = true;
                            }
                        }
                    }
                } else if (query instanceof CloseStep) {
                    CloseStep closeStep = (CloseStep) query;
                    if (closeStep.getCode() == 'S') {
                        if (lastParseStepIndexes.containsKey(closeStep.getName())) {
                            // Parse step was within buffered queries
                            refIndex = lastParseStepIndexes.get(closeStep.getName());
                            // Operation was already built
                            lastOutboundDataOperation = referencedOutboundDataOperations.get(refIndex);
                        } else {
                            // Parse step was done long time ago (and is tracked
                            // by
                            // session)
                            ExtendedQueryStatus<ParseStep> parseStepStatus = session
                                    .getParseStepStatus(closeStep.getName());
                            if (parseStepStatus != null) {
                                ParseStep parseStep = parseStepStatus.getQuery();
                                // Parse SQL statement
                                refStmt = parseSQL(ctx, parseStep.getSQL());
                                extract = true;
                            }
                        }
                        // Decrement parse step counter
                        Integer counter = parseStepCounters.get(closeStep.getName());
                        parseStepCounters.put(closeStep.getName(), counter - 1);
                    } else {
                        BindStep bindStep = null;
                        if (lastBindStepIndexes.containsKey(closeStep.getName())) {
                            // Bind step was within buffered queries
                            refIndex = lastBindStepIndexes.get(closeStep.getName());
                            // Operation was already built
                            lastOutboundDataOperation = referencedOutboundDataOperations.get(refIndex);
                        } else {
                            // Bind step was done long time ago (and is tracked
                            // by
                            // session)
                            ExtendedQueryStatus<BindStep> bindStepStatus = session
                                    .getBindStepStatus(closeStep.getName());
                            if (bindStepStatus != null) {
                                bindStep = bindStepStatus.getQuery();
                                ExtendedQueryStatus<ParseStep> parseStepStatus = session
                                        .getParseStepStatus(bindStep.getName());
                                if (parseStepStatus != null) {
                                    ParseStep parseStep = parseStepStatus.getQuery();
                                    // Parse SQL statement
                                    refStmt = parseSQL(ctx, parseStep.getSQL());
                                    // Build ParameterValue (type+format+value)
                                    refParamVals = bindStep.getParameterValues().stream()
                                            // transform each parameter to a
                                            // ParameterValue
                                            .map(ParameterValue::new)
                                            // save as a list
                                            .collect(Collectors.toList());
                                    final List<Long> parameterTypes = parseStep.getParameterTypes();
                                    for (int idx = 0; idx < parameterTypes.size(); idx++) {
                                        // set parameter type
                                        refParamVals.get(idx).setType(parameterTypes.get(idx));
                                    }
                                    final List<Short> formats = bindStep.getParameterFormats();
                                    for (int i = 0; i < refParamVals.size(); i++) {
                                        ParameterValue parameterValue = refParamVals.get(i);
                                        Short format = formats.get(formats.size() == 1 ? 0 : i);
                                        // set parameter value format
                                        parameterValue.setFormat(format);
                                    }
                                    // Ready to extract and protect data for
                                    // this query
                                    extract = true;
                                }
                            }
                        }
                        // Decrement bind step counter
                        Integer counter = bindStepCounters.get(closeStep.getName());
                        bindStepCounters.put(closeStep.getName(), counter - 1);
                    }
                } else if (query instanceof SynchronizeStep) {
                    if (queryIndex > 0) {
                        // Previous operation is the reference
                        refIndex = queryIndex - 1;
                        // Operation was already built
                        lastOutboundDataOperation = referencedOutboundDataOperations.get(refIndex);
                    }
                } else if (query instanceof FlushStep) {
                    if (queryIndex > 0) {
                        // Previous operation is the reference
                        refIndex = queryIndex - 1;
                        // Operation was already built
                        lastOutboundDataOperation = referencedOutboundDataOperations.get(refIndex);
                    }
                }
            }
            referencedStatements.add(refStmt);
            referencedParameterValues.add(refParamVals);
            referencedRows.add(refRow);
            if (extract) {
                // Extract module operation
                ModuleOperation moduleOperation = null;
                try {
                    if (refStmt instanceof SetStatement) {
                        moduleOperation = extractSetOperation(ctx, (SetStatement) refStmt);
                    } else if (refStmt instanceof StartTransaction) {
                        moduleOperation = extractStartTransactionOperation(ctx, (StartTransaction) refStmt);
                    } else if (refStmt instanceof Commit) {
                        moduleOperation = extractCommitOperation(ctx, (Commit) refStmt);
                    } else if (refStmt instanceof CreateTable) {
                        moduleOperation = extractCreateTableOperation(ctx, (CreateTable) refStmt);
                    } else if (refStmt instanceof Alter) {
                        // Retrieve the outbound data operation (if exist)
                        OutboundDataOperation refOutboundDataOperation = null;
                        if (lastOutboundDataOperation != null
                                && (lastOutboundDataOperation.getOperation() == Operation.CREATE
                                        || lastOutboundDataOperation.getOperation() == Operation.UPDATE)
                                && lastOutboundDataOperation.isUsingHeadOperation()) {
                            refOutboundDataOperation = lastOutboundDataOperation;
                        }
                        moduleOperation = extractAlterTableOperation(ctx, (Alter) refStmt, refOutboundDataOperation,
                                null);
                    } else if (refStmt instanceof CreateIndex) {
                        // Retrieve the outbound data operation (if exist)
                        OutboundDataOperation refOutboundDataOperation = null;
                        if (lastOutboundDataOperation != null
                                && (lastOutboundDataOperation.getOperation() == Operation.CREATE
                                        || lastOutboundDataOperation.getOperation() == Operation.UPDATE)
                                && lastOutboundDataOperation.isUsingHeadOperation()) {
                            refOutboundDataOperation = lastOutboundDataOperation;
                        }
                        moduleOperation = extractCreateIndexOperation(ctx, (CreateIndex) refStmt,
                                refOutboundDataOperation, null);
                    } else if (refStmt instanceof Drop) {
                        moduleOperation = extractDropTableOperation(ctx, (Drop) refStmt);
                    } else if (refStmt instanceof Insert) {
                        // Retrieve the outbound data operation (if exist)
                        OutboundDataOperation refOutboundDataOperation = null;
                        if (lastOutboundDataOperation != null
                                && lastOutboundDataOperation.getOperation() == Operation.CREATE
                                && !lastOutboundDataOperation.isUsingHeadOperation()) {
                            refOutboundDataOperation = lastOutboundDataOperation;
                        }
                        moduleOperation = extractInsertOperation(ctx, (Insert) refStmt, refParamVals,
                                refOutboundDataOperation);
                    } else if (refStmt instanceof Select) {
                        // Retrieve the outbound data operation (if exist)
                        OutboundDataOperation refOutboundDataOperation = null;
                        if (lastOutboundDataOperation != null
                                && (lastOutboundDataOperation.getOperation() == Operation.CREATE
                                        || lastOutboundDataOperation.getOperation() == Operation.UPDATE)
                                && lastOutboundDataOperation.isUsingHeadOperation()) {
                            refOutboundDataOperation = lastOutboundDataOperation;
                        }
                        moduleOperation = extractSelectOperation(ctx, (Select) refStmt, refParamVals,
                                refOutboundDataOperation, null);
                    } else if (refStmt instanceof DeclareCursor) {
                        moduleOperation = extractDeclareCursorOperation(ctx, (DeclareCursor) refStmt, refParamVals);
                    } else if (refStmt instanceof CursorFetch) {
                        moduleOperation = extractCursorFetchOperation(ctx, (CursorFetch) refStmt);
                    } else if (refStmt instanceof CursorClose) {
                        moduleOperation = extractCursorCloseOperation(ctx, (CursorClose) refStmt);
                    }
                } catch (ParseException e) {
                    // Currently, only thrown in case of error with a clarus
                    // function in the request
                    LOGGER.error("Parsing error for {} : ", refStmt);
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Parsing error details:", e);
                    }
                }
                if (moduleOperation instanceof OutboundDataOperation) {
                    lastOutboundDataOperation = (OutboundDataOperation) moduleOperation;
                } else if (moduleOperation instanceof InboundDataOperation) {
                    // Unsupported inbound data operation in buffered queries
                    // Should not occur
                    throw new IOException("Inbound operation is unsupported in buffered queries");
                } else if (moduleOperation instanceof MetadataOperation) {
                    // Unsupported metadata in buffered queries
                    // Should not occur
                    throw new IOException("Metadata operation is unsupported in buffered queries");
                } else if (moduleOperation != null) {
                    // Should not occur
                    throw new IOException(String.format("unsupported module operation type (%s) in buffered queries",
                            moduleOperation.getClass().getName()));
                }
            }
            referencedOutboundDataOperations.add(lastOutboundDataOperation);
            if (refIndex != -1) {
                referencedOutboundDataOperations.set(refIndex, lastOutboundDataOperation);
            }
            queryIndex++;
        }
        // Close parse steps and bind steps if their counter is negative or zero
        bindStepCounters.forEach((name, counter) -> {
            if (counter <= 0) {
                session.removeBindStep(name);
            }
        });
        parseStepCounters.forEach((name, counter) -> {
            if (counter <= 0) {
                session.removeParseStep(name);
            }
        });
        // Process outbound data operation
        boolean modifyRequests = false;
        Map<OutboundDataOperation, List<OutboundDataOperation>> dataOperation2newDataOperations = new HashMap<>();
        for (OutboundDataOperation outboundDataOperation : referencedOutboundDataOperations) {
            if (outboundDataOperation != null) {
                List<OutboundDataOperation> newOutboundDataOperations = dataOperation2newDataOperations
                        .get(outboundDataOperation);
                if (newOutboundDataOperations == null) {
                    newOutboundDataOperations = newOutboundDataOperation(ctx, outboundDataOperation);
                    dataOperation2newDataOperations.put(outboundDataOperation, newOutboundDataOperations);
                    modifyRequests |= newOutboundDataOperations.stream().anyMatch(DataOperation::isModified);
                }
            }
        }
        List<List<Query>> newQueries = new ArrayList<>();
        if (modifyRequests) {
            // Modify SQL queries (and parameter values)
            queryIndex = 0;
            for (Query bufferedQuery : session.getBufferedQueries()) {
                List<OutboundDataOperation> newOutboundDataOperations = null;
                OutboundDataOperation outboundDataOperation = referencedOutboundDataOperations.get(queryIndex);
                if (outboundDataOperation != null) {
                    newOutboundDataOperations = dataOperation2newDataOperations.get(outboundDataOperation);
                }
                boolean modifyRequest = newOutboundDataOperations != null && (FORCE_SQL_PROCESSING
                        || newOutboundDataOperations.size() != 1 || newOutboundDataOperations.get(0).isModified());
                if (modifyRequest) {
                    Statement refStmt = referencedStatements.get(queryIndex);
                    for (OutboundDataOperation newOutboundDataOperation : newOutboundDataOperations) {
                        Query newQuery = bufferedQuery;
                        if (refStmt != null) {
                            // Modify SQL statement
                            List<ParameterValue> refParamVals = referencedParameterValues.get(queryIndex);
                            Integer refRow = referencedRows.get(queryIndex);
                            Statement newStmt = refStmt;
                            if (refStmt instanceof SetStatement) {
                                PgsqlStatement<SetStatement> statement = new PgsqlStatement<>((SetStatement) refStmt,
                                        null, null, refParamVals, null, null);
                                PgsqlStatement<SetStatement> newStatement = modifySetStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                newStmt = newStatement.getStatement();
                            } else if (refStmt instanceof StartTransaction) {
                                PgsqlStatement<StartTransaction> statement = new PgsqlStatement<>(
                                        (StartTransaction) refStmt, null, null, refParamVals, null, null);
                                PgsqlStatement<StartTransaction> newStatement = modifyStartTransactionStatement(ctx,
                                        statement, newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                newStmt = newStatement.getStatement();
                            } else if (refStmt instanceof Commit) {
                                PgsqlStatement<Commit> statement = new PgsqlStatement<>((Commit) refStmt, null, null,
                                        refParamVals, null, null);
                                PgsqlStatement<Commit> newStatement = modifyCommitStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                newStmt = newStatement.getStatement();
                            } else if (refStmt instanceof CreateTable) {
                                PgsqlStatement<CreateTable> statement = new PgsqlStatement<>((CreateTable) refStmt,
                                        null, null, refParamVals, null, null);
                                PgsqlStatement<CreateTable> newStatement = modifyCreateTableStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                newStmt = newStatement.getStatement();
                            } else if (refStmt instanceof Alter) {
                                PgsqlStatement<Alter> statement = new PgsqlStatement<>((Alter) refStmt, null, null,
                                        refParamVals, null, null);
                                PgsqlStatement<Alter> newStatement = modifyAlterTableStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                newStmt = newStatement.getStatement();
                            } else if (refStmt instanceof CreateIndex) {
                                PgsqlStatement<CreateIndex> statement = new PgsqlStatement<>((CreateIndex) refStmt,
                                        null, null, refParamVals, null, null);
                                PgsqlStatement<CreateIndex> newStatement = modifyCreateIndexStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                newStmt = newStatement.getStatement();
                            } else if (refStmt instanceof Drop) {
                                PgsqlStatement<Drop> statement = new PgsqlStatement<>((Drop) refStmt, null, null,
                                        refParamVals, null, null);
                                PgsqlStatement<Drop> newStatement = modifyDropTableStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                newStmt = newStatement.getStatement();
                            } else if (refStmt instanceof Insert) {
                                PgsqlStatement<Insert> statement = new PgsqlStatement<>((Insert) refStmt, null, null,
                                        refParamVals, null, null);
                                PgsqlStatement<Insert> newStatement = modifyInsertStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1, refRow);
                                newStmt = newStatement.getStatement();
                            } else if (refStmt instanceof Select) {
                                PgsqlStatement<Select> statement = new PgsqlStatement<>((Select) refStmt, null, null,
                                        refParamVals, null, null);
                                PgsqlStatement<Select> newStatement = modifySelectStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1, null);
                                newStmt = newStatement.getStatement();
                            } else if (refStmt instanceof DeclareCursor) {
                                PgsqlStatement<DeclareCursor> statement = new PgsqlStatement<>((DeclareCursor) refStmt,
                                        null, null, refParamVals, null, null);
                                PgsqlStatement<DeclareCursor> newStatement = modifyDeclareCursorStatement(ctx,
                                        statement, newOutboundDataOperation, newOutboundDataOperations.size() > 1,
                                        null);
                                newStmt = newStatement.getStatement();
                            } else if (refStmt instanceof CursorFetch) {
                                PgsqlStatement<CursorFetch> statement = new PgsqlStatement<>((CursorFetch) refStmt,
                                        null, null, refParamVals, null, null);
                                PgsqlStatement<CursorFetch> newStatement = modifyCursorFetchStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                newStmt = newStatement.getStatement();
                            } else if (refStmt instanceof CursorClose) {
                                PgsqlStatement<CursorClose> statement = new PgsqlStatement<>((CursorClose) refStmt,
                                        null, null, refParamVals, null, null);
                                PgsqlStatement<CursorClose> newStatement = modifyCursorCloseStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                newStmt = newStatement.getStatement();
                            }
                            if (bufferedQuery instanceof SimpleQuery) {
                                SimpleSQLStatement simpleSQLStatement = (SimpleSQLStatement) bufferedQuery;
                                String newSQL = newStmt.toString();
                                newSQL = StringUtilities.addIrrelevantCharacters(newSQL, simpleSQLStatement.getSQL(),
                                        " \t\r\n;");
                                newQuery = new SimpleSQLStatement(CString.valueOf(newSQL));
                            } else {
                                ExtendedQuery query = (ExtendedQuery) bufferedQuery;
                                if (query instanceof ParseStep) {
                                    ParseStep parseStep = (ParseStep) query;
                                    String newSQL = newStmt.toString();
                                    newSQL = StringUtilities.addIrrelevantCharacters(newSQL, parseStep.getSQL(),
                                            " \t\r\n;");
                                    newQuery = new ParseStep(parseStep.getName(), CString.valueOf(newSQL),
                                            parseStep.isMetadata(), parseStep.getColumns(),
                                            parseStep.getParameterTypes());
                                } else if (query instanceof BindStep) {
                                    BindStep bindStep = (BindStep) query;
                                    List<ByteBuf> parameterBinaryValues = refParamVals.stream()
                                            // get the parameter value
                                            .map(ParameterValue::getValue)
                                            // save as a list
                                            .collect(Collectors.toList());
                                    if (!parameterBinaryValues.equals(bindStep.getParameterValues())) {
                                        // At least one parameter ByteBuf values
                                        // has
                                        // been changed
                                        newQuery = new BindStep(bindStep.getName(), bindStep.getPreparedStatement(),
                                                bindStep.getParameterFormats(), parameterBinaryValues,
                                                bindStep.getResultColumnFormats());
                                    }
                                }
                            }
                        }
                        int involvedBackend = newOutboundDataOperation.getInvolvedCSP();
                        if (involvedBackend == -1) {
                            involvedBackend = getPreferredBackend(ctx);
                        }
                        if (newQueries.size() <= involvedBackend) {
                            for (int i = newQueries.size(); i <= involvedBackend; i++) {
                                newQueries.add(null);
                            }
                        }
                        List<Query> newBackendQueries = newQueries.get(involvedBackend);
                        if (newBackendQueries == null) {
                            newBackendQueries = new ArrayList<>();
                            newQueries.set(involvedBackend, newBackendQueries);
                        }
                        newQuery.retain();
                        newBackendQueries.add(newQuery);
                    }
                } else {
                    int involvedBackend = newOutboundDataOperations != null && newOutboundDataOperations.size() == 1
                            ? newOutboundDataOperations.get(0).getInvolvedCSP() : -1;
                    if (involvedBackend == -1) {
                        involvedBackend = getPreferredBackend(ctx);
                    }
                    if (newQueries.size() <= involvedBackend) {
                        for (int i = newQueries.size(); i <= involvedBackend; i++) {
                            newQueries.add(null);
                        }
                    }
                    List<Query> newBackendQueries = newQueries.get(involvedBackend);
                    if (newBackendQueries == null) {
                        newBackendQueries = new ArrayList<>();
                        newQueries.set(involvedBackend, newBackendQueries);
                    }
                    bufferedQuery.retain();
                    newBackendQueries.add(bufferedQuery);
                }
                queryIndex++;
            }
        } else if (!session.getBufferedQueries().isEmpty()) {
            int involvedBackend = getPreferredBackend(ctx);
            if (newQueries.size() <= involvedBackend) {
                for (int i = newQueries.size(); i <= involvedBackend; i++) {
                    newQueries.add(null);
                }
            }
            session.getBufferedQueries().forEach(q -> q.retain());
            newQueries.set(involvedBackend, session.getBufferedQueries());
        }
        session.resetBufferedQueries();
        return newQueries;
    }

    private static class Result<Q, R, E> {
        private final Optional<List<Q>> queries;
        private final Optional<R> response;
        private final Optional<E> error;

        public static <Q, R, E> Result<Q, R, E> query(Q query) {
            return queries(Collections.singletonList(query));
        }

        public static <Q, R, E> Result<Q, R, E> queries(List<Q> queries) {
            return new Result<>(Optional.ofNullable(queries), null, null);
        }

        public static <Q, R, E> Result<Q, R, E> response(R response) {
            return new Result<>(null, Optional.ofNullable(response), null);
        }

        public static <Q, R, E> Result<Q, R, E> error(E error) {
            return new Result<>(null, null, Optional.ofNullable(error));
        }

        private Result(Optional<List<Q>> queries, Optional<R> response, Optional<E> error) {
            this.queries = queries;
            this.response = response;
            this.error = error;
        }

        public List<Q> queries() {
            return queries == null ? null : queries.orElse(null);
        }

        public Map<Integer, Q> queriesAsMap() {
            return queries == null || !queries.isPresent() ? null
                    : IntStream.range(0, queries.get().size()).mapToObj(i -> new SimpleEntry<>(i, queries.get().get(i)))
                            .filter(e -> e.getValue() != null)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public R response() {
            return response == null ? null : response.orElse(null);
        }

        public E error() {
            return error == null ? null : error.orElse(null);
        }

        public boolean isQuery() {
            return queries != null;
        }

        public boolean isResponse() {
            return response != null;
        }

        public boolean isError() {
            return error != null;
        }
    }

    private Result<SimpleSQLStatement, CommandResults, CString> processSimpleSQLStatement(ChannelHandlerContext ctx,
            SimpleSQLStatement sqlStatement, Operation operation) {
        // Parse SQL statement
        Statement stmt = parseSQL(ctx, sqlStatement.getSQL());
        LOGGER.debug("statement type: {}", stmt != null ? stmt.getClass().getSimpleName() : null);
        if (stmt == null) {
            return Result.query(sqlStatement);
        }
        // Extract module operation
        ModuleOperation moduleOperation = null;
        try {
            if (stmt instanceof SetStatement) {
                moduleOperation = extractSetOperation(ctx, (SetStatement) stmt);
            } else if (stmt instanceof StartTransaction) {
                moduleOperation = extractStartTransactionOperation(ctx, (StartTransaction) stmt);
            } else if (stmt instanceof Commit) {
                moduleOperation = extractCommitOperation(ctx, (Commit) stmt);
            } else if (stmt instanceof CreateTable) {
                moduleOperation = extractCreateTableOperation(ctx, (CreateTable) stmt);
            } else if (stmt instanceof Alter) {
                moduleOperation = extractAlterTableOperation(ctx, (Alter) stmt, null, operation);
            } else if (stmt instanceof CreateIndex) {
                moduleOperation = extractCreateIndexOperation(ctx, (CreateIndex) stmt, null, operation);
            } else if (stmt instanceof Drop) {
                moduleOperation = extractDropTableOperation(ctx, (Drop) stmt);
            } else if (stmt instanceof Insert) {
                moduleOperation = extractInsertOperation(ctx, (Insert) stmt, null, null);
            } else if (stmt instanceof Update) {
                // TODO Update
            } else if (stmt instanceof Delete) {
                // TODO Delete
            } else if (stmt instanceof Select) {
                moduleOperation = extractSelectOperation(ctx, (Select) stmt, null, null, operation);
            } else if (stmt instanceof DeclareCursor) {
                moduleOperation = extractDeclareCursorOperation(ctx, (DeclareCursor) stmt, null);
            } else if (stmt instanceof CursorFetch) {
                moduleOperation = extractCursorFetchOperation(ctx, (CursorFetch) stmt);
            } else if (stmt instanceof CursorClose) {
                moduleOperation = extractCursorCloseOperation(ctx, (CursorClose) stmt);
            }
        } catch (ParseException e) {
            return Result.error(CString.valueOf(e.getMessage()));
        }
        SQLSession session = getSession(ctx);
        Result<SimpleSQLStatement, CommandResults, CString> result = null;
        if (moduleOperation instanceof MetadataOperation) {
            // Process metadata operation
            MetadataOperation metadataOperation = (MetadataOperation) moduleOperation;
            // Save involved backends
            List<Integer> involvedBackends = metadataOperation.getInvolvedCSPs();
            if (involvedBackends == null) {
                involvedBackends = Collections.emptyList();
            }
            session.setCommandInvolvedBackends(involvedBackends);
            metadataOperation = newMetaDataOperation(ctx, metadataOperation);
            // Forget SQL statement, reply directly to the frontend
            CommandResults commandResults = new CommandResults();
            // Build row description
            List<PgsqlRowDescriptionMessage.Field> description = Stream.of("column_name", "protected_column_name")
                    .map(CString::valueOf).map(PgsqlRowDescriptionMessage.Field::new).collect(Collectors.toList());
            commandResults.setRowDescription(description);
            if (metadataOperation.isModified()) {
                List<Map.Entry<CString, List<CString>>> metadata = metadataOperation.getMetadata();
                // Verify all data ids refer to the same dataset (prefix is the
                // same for all data ids)
                Set<CString> prefixes = metadata.stream().map(Map.Entry::getKey)
                        .map(id -> id.substring(0, id.lastIndexOf('/'))).collect(Collectors.toSet());
                boolean multipleDatasets = prefixes.size() > 1 || prefixes.stream().findFirst().get().equals("*");
                if (!multipleDatasets) {
                    // Prefix is the same for all data ids -> remove prefix
                    metadata = metadata.stream().map(
                            e -> new SimpleEntry<>(e.getKey().substring(e.getKey().lastIndexOf('/') + 1), e.getValue()))
                            .collect(Collectors.toList());
                }
                // Build rows
                List<List<ByteBuf>> rows = buildRows(metadata, involvedBackends, -1);
                commandResults.setRows(rows);
                // Build complete tag
                commandResults.setCompleteTag(CString.valueOf("SELECT " + rows.size()));
            } else {
                commandResults.setCompleteTag(CString.valueOf("SELECT 0"));
            }
            result = Result.response(commandResults);
            // reset current command in session
            session.resetCurrentCommand();
        } else if (moduleOperation instanceof OutboundDataOperation) {
            OutboundDataOperation outboundDataOperation = (OutboundDataOperation) moduleOperation;
            // Process data operation
            List<OutboundDataOperation> newOutboundDataOperations = newOutboundDataOperation(ctx,
                    outboundDataOperation);
            if (stmt instanceof SetStatement || stmt instanceof StartTransaction || stmt instanceof Commit
                    || stmt instanceof CreateTable || stmt instanceof Alter || stmt instanceof CreateIndex
                    || stmt instanceof Drop || stmt instanceof Insert || stmt instanceof Select
                    || stmt instanceof DeclareCursor || stmt instanceof CursorFetch || stmt instanceof CursorClose) {
                if (newOutboundDataOperations.isEmpty()) {
                    // Forget SQL statement, reply directly to the frontend
                    CommandResults commandResults = new CommandResults();
                    if (stmt instanceof SetStatement) {
                        commandResults.setCompleteTag(CString.valueOf("SET"));
                    } else if (stmt instanceof StartTransaction) {
                        commandResults.setCompleteTag(CString.valueOf("BEGIN"));
                    } else if (stmt instanceof Commit) {
                        commandResults.setCompleteTag(CString.valueOf("COMMIT"));
                    } else if (stmt instanceof CreateTable) {
                        commandResults.setCompleteTag(CString.valueOf("CREATE TABLE"));
                    } else if (stmt instanceof Alter) {
                        commandResults.setCompleteTag(CString.valueOf("ALTER TABLE"));
                    } else if (stmt instanceof CreateIndex) {
                        commandResults.setCompleteTag(CString.valueOf("CREATE INDEX"));
                    } else if (stmt instanceof Drop) {
                        commandResults.setCompleteTag(CString.valueOf("DROP TABLE"));
                    } else if (stmt instanceof Insert) {
                        commandResults.setCompleteTag(CString.valueOf("INSERT 0 1"));
                    } else if (stmt instanceof Update) {
                        commandResults.setCompleteTag(CString.valueOf("UPDATE 0 1"));
                    } else if (stmt instanceof Delete) {
                        commandResults.setCompleteTag(CString.valueOf("DELETE 0 1"));
                    } else if (stmt instanceof Select) {
                        commandResults.setCompleteTag(CString.valueOf("SELECT 0"));
                    } else if (stmt instanceof DeclareCursor) {
                        commandResults.setCompleteTag(CString.valueOf("DECLARE CURSOR"));
                    } else if (stmt instanceof CursorFetch) {
                        commandResults.setCompleteTag(CString.valueOf("FETCH 0"));
                    } else if (stmt instanceof CursorClose) {
                        commandResults.setCompleteTag(CString.valueOf("CLOSE CURSOR"));
                    }
                    result = Result.response(commandResults);
                    session.setCommandInvolvedBackends(Collections.emptyList());
                    // reset current command in session
                    session.resetCurrentCommand();
                } else {
                    List<Integer> involvedBackends;
                    List<ExpectedField> expectedFields = null;
                    boolean requestModified = newOutboundDataOperations.size() > 1
                            || newOutboundDataOperations.get(0).isModified();
                    if (FORCE_SQL_PROCESSING || requestModified) {
                        List<SimpleSQLStatement> sqlStatements = new ArrayList<>(newOutboundDataOperations.size());
                        involvedBackends = new ArrayList<>(newOutboundDataOperations.size());
                        if (session.getCurrentCommandOperation() == Operation.READ) {
                            expectedFields = new ArrayList<>();
                        }
                        // Modify SQL statement
                        for (OutboundDataOperation newOutboundDataOperation : newOutboundDataOperations) {
                            String newSQL;
                            if (stmt instanceof SetStatement) {
                                PgsqlStatement<SetStatement> statement = new PgsqlStatement<>((SetStatement) stmt);
                                PgsqlStatement<SetStatement> newStatement = modifySetStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                SetStatement newSetStatement = newStatement.getStatement();
                                newSQL = newSetStatement.toString();
                            } else if (stmt instanceof StartTransaction) {
                                PgsqlStatement<StartTransaction> statement = new PgsqlStatement<>(
                                        (StartTransaction) stmt);
                                PgsqlStatement<StartTransaction> newStatement = modifyStartTransactionStatement(ctx,
                                        statement, newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                StartTransaction newStartTransaction = newStatement.getStatement();
                                newSQL = newStartTransaction.toString();
                            } else if (stmt instanceof Commit) {
                                PgsqlStatement<Commit> statement = new PgsqlStatement<>((Commit) stmt);
                                PgsqlStatement<Commit> newStatement = modifyCommitStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                Commit newCommit = newStatement.getStatement();
                                newSQL = newCommit.toString();
                            } else if (stmt instanceof CreateTable) {
                                PgsqlStatement<CreateTable> statement = new PgsqlStatement<>((CreateTable) stmt);
                                PgsqlStatement<CreateTable> newStatement = modifyCreateTableStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                CreateTable newCreateTable = newStatement.getStatement();
                                newSQL = newCreateTable.toString();
                            } else if (stmt instanceof Alter) {
                                PgsqlStatement<Alter> statement = new PgsqlStatement<>((Alter) stmt);
                                PgsqlStatement<Alter> newStatement = modifyAlterTableStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                Alter newAlter = newStatement.getStatement();
                                newSQL = newAlter.toString();
                            } else if (stmt instanceof CreateIndex) {
                                PgsqlStatement<CreateIndex> statement = new PgsqlStatement<>((CreateIndex) stmt);
                                PgsqlStatement<CreateIndex> newStatement = modifyCreateIndexStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                CreateIndex newCreateIndex = newStatement.getStatement();
                                newSQL = newCreateIndex.toString();
                            } else if (stmt instanceof Drop) {
                                PgsqlStatement<Drop> statement = new PgsqlStatement<>((Drop) stmt);
                                PgsqlStatement<Drop> newStatement = modifyDropTableStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                Drop newDropTable = newStatement.getStatement();
                                newSQL = newDropTable.toString();
                            } else if (stmt instanceof Insert) {
                                PgsqlStatement<Insert> statement = new PgsqlStatement<>((Insert) stmt);
                                PgsqlStatement<Insert> newStatement = modifyInsertStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1, 0);
                                Insert newInsert = newStatement.getStatement();
                                newSQL = newInsert.toString();
                            } else if (stmt instanceof Select) {
                                PgsqlStatement<Select> statement = new PgsqlStatement<>((Select) stmt);
                                PgsqlStatement<Select> newStatement = modifySelectStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1, expectedFields);
                                Select newSelect = newStatement.getStatement();
                                newSQL = newSelect.toString();
                            } else if (stmt instanceof DeclareCursor) {
                                PgsqlStatement<DeclareCursor> statement = new PgsqlStatement<>((DeclareCursor) stmt);
                                PgsqlStatement<DeclareCursor> newStatement = modifyDeclareCursorStatement(ctx,
                                        statement, newOutboundDataOperation, newOutboundDataOperations.size() > 1,
                                        expectedFields);
                                DeclareCursor newDeclareCursor = newStatement.getStatement();
                                newSQL = newDeclareCursor.toString();
                            } else if (stmt instanceof CursorFetch) {
                                PgsqlStatement<CursorFetch> statement = new PgsqlStatement<>((CursorFetch) stmt);
                                PgsqlStatement<CursorFetch> newStatement = modifyCursorFetchStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                CursorFetch newCursorFetch = newStatement.getStatement();
                                newSQL = newCursorFetch.toString();
                            } else if (stmt instanceof CursorClose) {
                                PgsqlStatement<CursorClose> statement = new PgsqlStatement<>((CursorClose) stmt);
                                PgsqlStatement<CursorClose> newStatement = modifyCursorCloseStatement(ctx, statement,
                                        newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                CursorClose newCursorClose = newStatement.getStatement();
                                newSQL = newCursorClose.toString();
                            } else {
                                newSQL = stmt.toString();
                            }
                            newSQL = StringUtilities.addIrrelevantCharacters(newSQL, sqlStatement.getSQL(), " \t\r\n;");
                            sqlStatement = new SimpleSQLStatement(CString.valueOf(newSQL));
                            int involvedBackend = newOutboundDataOperation.getInvolvedCSP();
                            if (involvedBackend == -1) {
                                involvedBackend = getPreferredBackend(ctx);
                            }
                            involvedBackends.add(involvedBackend);
                            if (sqlStatements.size() <= involvedBackend) {
                                for (int i = sqlStatements.size(); i <= involvedBackend; i++) {
                                    sqlStatements.add(null);
                                }
                            }
                            sqlStatements.set(involvedBackend, sqlStatement);
                        }
                        result = Result.queries(sqlStatements);
                    } else {
                        involvedBackends = newOutboundDataOperations.get(0).getInvolvedCSPs();
                        if (involvedBackends == null) {
                            involvedBackends = Collections.singletonList(getPreferredBackend(ctx));
                        }
                        result = Result.query(sqlStatement);
                        if (session.getCurrentCommandOperation() == Operation.READ) {
                            @SuppressWarnings("unchecked")
                            List<Map.Entry<SelectItem, List<String>>> selectItemIds = (List<Map.Entry<SelectItem, List<String>>>) outboundDataOperation
                                    .getAttribute("selectItemIds");
                            expectedFields = selectItemIds.stream().map(e -> {
                                SelectItem item = e.getKey();
                                String fqOutputName = toOutputName(item.toString());
                                List<String> attributeNames = e.getValue();
                                List<Map.Entry<String, Integer>> attributeMapping = IntStream
                                        .range(0, attributeNames.size())
                                        .mapToObj(i -> new SimpleEntry<>(attributeNames.get(i), i))
                                        .collect(Collectors.toList());
                                Map<Integer, List<ExpectedProtectedField>> protectedFields = Stream
                                        .of(new ExpectedProtectedField(0, fqOutputName, attributeNames,
                                                attributeMapping))
                                        .collect(Collectors.groupingBy(ExpectedProtectedField::getBackend));
                                return new ExpectedField(fqOutputName, attributeNames, protectedFields);
                            }).collect(Collectors.toList());
                        }
                    }
                    session.setCommandInvolvedBackends(involvedBackends);
                    if (session.getCurrentCommandOperation() == Operation.READ) {
                        session.setResultProcessingEnabled(
                                requestModified || !outboundDataOperation.isUnprotectingDataEnabled());
                        session.setPromise(newOutboundDataOperations.get(0).getPromise());
                        session.setUnprotectingDataEnabled(outboundDataOperation.isUnprotectingDataEnabled());
                        session.setExpectedFields(expectedFields);
                        if (stmt instanceof DeclareCursor) {
                            // Save cursor context
                            session.saveCursorContext(((DeclareCursor) stmt).getName());
                        }
                    } else {
                        if (stmt instanceof CursorFetch) {
                            // Restore cursor context
                            session.restoreCursorContext(((CursorFetch) stmt).getName());
                        } else if (stmt instanceof CursorClose) {
                            // Remove cursor context
                            session.removeCursorContext(((CursorClose) stmt).getName());
                        }
                    }
                }
            } else if (stmt instanceof Update) {
                // TODO Update
                result = Result.query(sqlStatement);
            } else if (stmt instanceof Delete) {
                // TODO Delete
                result = Result.query(sqlStatement);
            } else {
                result = Result.query(sqlStatement);
            }
        } else {
            result = Result.query(sqlStatement);
        }
        return result;
    }

    private MetadataOperation newMetaDataOperation(ChannelHandlerContext ctx, MetadataOperation metadataOperation) {
        MetadataOperation newMetaDataOperation = getProtocolService(ctx).newMetadataOperation(metadataOperation);
        int maxBackend = newMetaDataOperation.getInvolvedCSPs().stream().max(Comparator.naturalOrder()).orElse(-1);
        String[] geometryColumnsDataids = Stream
                .of("f_table_catalog", "f_table_schema", "f_table_name", "f_geometry_column", "type")
                .map(cn -> "geometry_columns/" + cn).toArray(String[]::new);
        String[] pgStatsDataids = Stream.of("schemaname", "tablename", "attname").map(cn -> "pg_stats/" + cn)
                .toArray(String[]::new);
        newMetaDataOperation.getMetadata().stream()
                .filter(e -> Stream.concat(Stream.of(geometryColumnsDataids), Stream.of(pgStatsDataids))
                        .anyMatch(cid -> e.getKey().endsWith(cid)))
                .filter(e -> e.getValue().isEmpty()).forEach(e -> {
                    e.setValue(IntStream.range(0, maxBackend + 1).mapToObj(
                            backend -> newMetaDataOperation.getInvolvedCSPs().contains(backend) ? e.getKey() : null)
                            .collect(Collectors.toList()));
                });
        List<String> backendDatabaseNames = getBackendDatabaseNames(ctx);
        if (!backendDatabaseNames.isEmpty()) {
            // Modify data ids (replace database name by the back-end database
            // name)
            newMetaDataOperation.getMetadata()
                    .forEach(e -> IntStream.range(0, e.getValue().size()).filter(csp -> e.getValue().get(csp) != null)
                            .forEach(csp -> e.getValue().set(csp,
                                    modifyDatabaseName(e.getValue().get(csp), backendDatabaseNames.get(csp)))));
        }
        return newMetaDataOperation;
    }

    private List<OutboundDataOperation> newOutboundDataOperation(ChannelHandlerContext ctx,
            OutboundDataOperation outboundDataOperation) {
        if (outboundDataOperation.getOperation() == Operation.READ
                && outboundDataOperation.getCriterions().stream().filter(c -> c != null)
                        .map(OutboundDataOperation.Criterion::getDataId).anyMatch(id -> id.endsWith("pg_type/oid"))) {
            SQLDatabaseSchema databaseSchema = getDatabaseSchema(ctx);
            List<OutboundDataOperation.Criterion> criterions = outboundDataOperation.getCriterions();
            List<Integer> involvedBackends = criterions.stream().filter(c -> c != null)
                    .filter(c -> c.getDataId().endsWith("pg_type/oid")).map(OutboundDataOperation.Criterion::getValue)
                    .map(CString::toString).mapToLong(Long::valueOf)
                    .mapToObj(oid -> databaseSchema.getTypeOIDBackends(oid)).filter(backends -> backends != null)
                    .flatMap(SortedSet::stream).distinct().collect(Collectors.toList());
            if (!involvedBackends.isEmpty()) {
                if (outboundDataOperation.getInvolvedCSPs() != null) {
                    involvedBackends.removeAll(outboundDataOperation.getInvolvedCSPs());
                }
                outboundDataOperation.setInvolvedCSPs(involvedBackends);
                outboundDataOperation.setModified(true);
            }
        }
        List<OutboundDataOperation> newOutboundDataOperations = getProtocolService(ctx)
                .newOutboundDataOperation(outboundDataOperation);
        List<String> backendDatabaseNames = getBackendDatabaseNames(ctx);
        if (!backendDatabaseNames.isEmpty()) {
            // Modify data ids (replace database name by the back-end database
            // name)
            newOutboundDataOperations.forEach(newOutboundDataOperation -> {
                int backend = newOutboundDataOperation.getInvolvedCSP() == -1 ? getPreferredBackend(ctx)
                        : newOutboundDataOperation.getInvolvedCSP();
                List<CString> dataIds = newOutboundDataOperation.getDataIds();
                IntStream.range(0, dataIds.size()).forEach(
                        i -> dataIds.set(i, modifyDatabaseName(dataIds.get(i), backendDatabaseNames.get(backend))));
                newOutboundDataOperation.getCriterions().stream().filter(c -> c != null).forEach(
                        c -> c.setDataId(modifyDatabaseName(c.getDataId(), backendDatabaseNames.get(backend))));
            });
        }
        @SuppressWarnings("unchecked")
        List<Map.Entry<SelectItem, List<String>>> selectItemIds = (List<Map.Entry<SelectItem, List<String>>>) outboundDataOperation
                .getAttribute("selectItemIds");
        if (selectItemIds != null) {
            selectItemIds.forEach(entry -> {
                SelectItem selectItem = entry.getKey();
                if (selectItem instanceof SelectExpressionItem
                        && ((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
                    Function function = (Function) ((SelectExpressionItem) selectItem).getExpression();
                    if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
                        // for each dataId, prepare the possible modification of the
                        // database name, of the schema name, of the table name and
                        // of the column name
                        List<String> dataIds = entry.getValue();
                        List<Pattern> dataIdPatterns = dataIds
                                .stream().map(
                                        dataId -> isFullyQualifiedDataId(dataId)
                                                ? Pattern.compile(String.format("(%s\\.?)?(%s\\.?)?(%s\\.?)?(%s)?",
                                                        getDatabaseName(dataId), getSchemaName(dataId),
                                                        getTableName(dataId), getColumnName(dataId)))
                                                : null)
                                .collect(Collectors.toList());
                        List<List<Matcher>> parameterMatchers = function.getParameters().getExpressions().stream()
                                .map(parameter -> parameter instanceof StringValue
                                        ? ((StringValue) parameter).getValue() : parameter.toString())
                                .map(StringUtilities::unquote).map(value -> value.replace("\".\"", "."))
                                .map(value -> dataIdPatterns.stream()
                                        .map(pattern -> pattern != null ? pattern.matcher(value) : null)
                                        .collect(Collectors.toList()))
                                .collect(Collectors.toList());
                        if (parameterMatchers.stream().flatMap(List::stream).filter(matcher -> matcher != null)
                                .anyMatch(Matcher::matches)) {
                            // prepare extraction functions
                            List<java.util.function.Function<String, String>> fctGetDataIdParts = Stream.<java.util.function.Function<String, String>>of(
                                    this::getDatabaseName, this::getSchemaName, this::getTableName, this::getColumnName)
                                    .collect(Collectors.toList());
                            for (int backend = 0; backend < newOutboundDataOperations.size(); backend++) {
                                OutboundDataOperation newOutboundDataOperation = newOutboundDataOperations.get(backend);
                                Map<CString, CString> dataIdMapping = newOutboundDataOperation.getDataIdMapping();
                                Map<String, String> mapping = dataIdMapping.entrySet().stream().collect(
                                        Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
                                List<CString> newDataValues = newOutboundDataOperation.getDataValues().get(0);
                                for (int c = 0; c < newDataValues.size(); c++) {
                                    CString newDataValue = newDataValues.get(c);
                                    if (newDataValue != null && newDataValue.equals(function.toString())) {
                                        String dataValue = newDataValue.toString();
                                        Function newFunction = null;
                                        CCJSqlParser parser = new CCJSqlParser(new StringReader(dataValue));
                                        try {
                                            newFunction = parser.Function();
                                        } catch (Exception e) {
                                            LOGGER.error("Parsing error for {} : ", dataValue, e);
                                        }
                                        if (newFunction != null && newFunction.getParameters() != null
                                                && newFunction.getParameters().getExpressions() != null) {
                                            for (int p = 0; p < newFunction.getParameters().getExpressions()
                                                    .size(); p++) {
                                                for (int i = 0; i < parameterMatchers.get(p).size(); i++) {
                                                    Matcher parameterMatcher = parameterMatchers.get(p).get(i);
                                                    if (parameterMatcher != null && parameterMatcher.matches()) {
                                                        String dataId = dataIds.get(i);
                                                        if (isFullyQualifiedDataId(dataId)) {
                                                            String protectedDataId = mapping.get(dataId);
                                                            if (isFullyQualifiedDataId(protectedDataId)) {
                                                                StringBuilder target = new StringBuilder();
                                                                StringBuilder replacement = new StringBuilder();
                                                                for (int g = 0; g < fctGetDataIdParts.size(); g++) {
                                                                    String group = parameterMatcher.group(g + 1);
                                                                    if (group != null && !group.isEmpty()) {
                                                                        java.util.function.Function<String, String> fctGetDataIdPart = fctGetDataIdParts
                                                                                .get(g);
                                                                        if (target.length() > 0) {
                                                                            target.append('.');
                                                                        }
                                                                        target.append(fctGetDataIdPart.apply(dataId));
                                                                        if (protectedDataId != null) {
                                                                            if (replacement.length() > 0) {
                                                                                replacement.append('.');
                                                                            }
                                                                            replacement.append(fctGetDataIdPart
                                                                                    .apply(protectedDataId));
                                                                        }
                                                                    }
                                                                }
                                                                Expression parameter = newFunction.getParameters()
                                                                        .getExpressions().get(p);
                                                                String value = parameter instanceof StringValue
                                                                        ? ((StringValue) parameter).getValue()
                                                                        : parameter.toString();
                                                                boolean withQuotes = StringUtilities.hasQuote(value);
                                                                if (withQuotes) {
                                                                    value = StringUtilities.unquote(value)
                                                                            .replace("\".\"", ".");
                                                                }
                                                                String newValue = value.replace(target.toString(),
                                                                        replacement.toString());
                                                                if (withQuotes) {
                                                                    newValue = StringUtilities
                                                                            .quote(newValue.replace(".", "\".\""));
                                                                }
                                                                Expression newParameter = buildExpression(parameter,
                                                                        value, newValue);
                                                                newFunction.getParameters().getExpressions().set(p,
                                                                        newParameter);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            String modifiedDataValue = newFunction.toString();
                                            newDataValues.set(c, CString.valueOf(modifiedDataValue));
                                            newOutboundDataOperation.setModified(true);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });
            if (outboundDataOperation.getOperation() == Operation.CREATE
                    || outboundDataOperation.getOperation() == Operation.UPDATE) {
                selectItemIds.forEach(entry -> {
                    SelectItem selectItem = entry.getKey();
                    if (selectItem instanceof SelectExpressionItem
                            && ((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
                        Function function = (Function) ((SelectExpressionItem) selectItem).getExpression();
                        if (FUNCTION_ADD_GEOMETRY_COLUMN.equalsIgnoreCase(function.getName())) {
                            // prepare the possible modification of the geometry
                            // type
                            String protectedType = null;
                            Map<String, String> geometryObjectDefinition = getGeometryObjectDefinition(ctx);
                            if (!geometryObjectDefinition.isEmpty()) {
                                protectedType = geometryObjectDefinition
                                        .get(PgsqlConfiguration.GEOMETRIC_OBJECT_PROTECTED_TYPE);
                            }
                            // modify the geometry type if necessary
                            if (protectedType != null) {
                                for (DataOperation newOutboundDataOperation : newOutboundDataOperations) {
                                    List<CString> newDataValues = newOutboundDataOperation.getDataValues().get(0);
                                    for (int c = 0; c < newDataValues.size(); c++) {
                                        CString newDataValue = newDataValues.get(c);
                                        if (newDataValue != null && newDataValue.startsWith(function.getName())) {
                                            String dataValue = newDataValue.toString();
                                            String modifiedDataValue = dataValue;
                                            for (GeometryType geometryType : GeometryType.values()) {
                                                String clearType = geometryType.toString();
                                                if (!clearType.equals(protectedType)) {
                                                    modifiedDataValue = modifiedDataValue
                                                            .replaceAll("(?i)\\b" + clearType + "\\b", protectedType);
                                                }
                                            }
                                            if (!modifiedDataValue.equals(dataValue)) {
                                                newDataValues.set(c, CString.valueOf(modifiedDataValue));
                                                newOutboundDataOperation.setModified(true);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }
        String[] geometryColumnsDataids = Stream
                .of("f_table_catalog", "f_table_schema", "f_table_name", "f_geometry_column", "type")
                .map(cn -> "geometry_columns/" + cn).toArray(String[]::new);
        if (outboundDataOperation.getOperation() == Operation.READ && (outboundDataOperation.getDataIds().stream()
                .anyMatch(id -> Stream.of(geometryColumnsDataids).anyMatch(cid -> id.endsWith(cid)))
                || outboundDataOperation.getCriterions().stream().filter(c -> c != null)
                        .map(OutboundDataOperation.Criterion::getDataId)
                        .anyMatch(id -> Stream.of(geometryColumnsDataids).anyMatch(cid -> id.endsWith(cid))))) {
            Map<String, String> geometryObjectDefinition = getGeometryObjectDefinition(ctx);
            if (!geometryObjectDefinition.isEmpty()) {
                boolean catalogProtected = backendDatabaseNames.stream()
                        .anyMatch(dbn -> !dbn.equals(getDatabaseName(ctx)));
                String geometricDataId = geometryObjectDefinition.get(PgsqlConfiguration.GEOMETRIC_DATA_ID);
                List<CString> protectedGeometricDataIds = null;
                if (geometricDataId != null) {
                    MetadataOperation metadataOperation = new MetadataOperation();
                    metadataOperation.addDataId(CString.valueOf(geometricDataId));
                    List<Entry<CString, List<CString>>> metadata = newMetaDataOperation(ctx, metadataOperation)
                            .getMetadata();
                    protectedGeometricDataIds = !metadata.isEmpty() ? metadata.get(0).getValue() : null;
                }
                String protectedGeometricDataId = protectedGeometricDataIds != null
                        && !protectedGeometricDataIds.isEmpty() ? protectedGeometricDataIds.get(0).toString() : null;
                boolean dataIdProtected = geometricDataId != null && protectedGeometricDataId != null
                        && !geometricDataId.equals(protectedGeometricDataId);
                String clearType = geometryObjectDefinition.get(PgsqlConfiguration.GEOMETRIC_OBJECT_CLEAR_TYPE);
                String protectedType = geometryObjectDefinition.get(PgsqlConfiguration.GEOMETRIC_OBJECT_PROTECTED_TYPE);
                boolean typeProtected = !Objects.equals(clearType, protectedType);
                if (catalogProtected || dataIdProtected || typeProtected) {
                    String catalogName = dataIdProtected && !getDatabaseName(geometricDataId).equals("*")
                            ? getDatabaseName(geometricDataId) : catalogProtected ? getDatabaseName(ctx) : "*";
                    String schemaName = dataIdProtected ? getSchemaName(geometricDataId) : "*";
                    String tableName = dataIdProtected ? getTableName(geometricDataId) : "*";
                    String columnName = dataIdProtected ? getColumnName(geometricDataId) : "*";
                    String protectedCatalogName = dataIdProtected ? getDatabaseName(protectedGeometricDataId)
                            : catalogProtected ? backendDatabaseNames.get(0) : "*";
                    String protectedSchemaName = dataIdProtected ? getSchemaName(protectedGeometricDataId) : "*";
                    String protectedTableName = dataIdProtected ? getTableName(protectedGeometricDataId) : "*";
                    String protectedColumnName = dataIdProtected ? getColumnName(protectedGeometricDataId) : "*";
                    boolean protectCatalogName = !protectedCatalogName.equals("*") && !catalogName.equals("*")
                            && !catalogName.equals(protectedCatalogName);
                    boolean protectSchemaName = !protectedSchemaName.equals("*") && !schemaName.equals("*")
                            && !schemaName.equals(protectedSchemaName);
                    boolean protectTableName = !protectedTableName.equals("*") && !tableName.equals("*")
                            && !tableName.equals(protectedTableName);
                    boolean protectColumnName = !protectedColumnName.equals("*") && !columnName.equals("*")
                            && !columnName.equals(protectedColumnName);
                    newOutboundDataOperations.stream().forEach(newOutboundDataOperation -> {
                        {
                            // process data identifiers
                            List<CString> dataIds = newOutboundDataOperation.getDataIds();
                            int[] catalogIndexes = catalogProtected ? IntStream.range(0, dataIds.size())
                                    .filter(i -> dataIds.get(i).endsWith("geometry_columns/f_table_catalog")).toArray()
                                    : new int[0];
                            int[] dataIdIndexes = dataIdProtected
                                    ? IntStream.range(0, dataIds.size())
                                            .filter(i -> Stream
                                                    .of("f_table_catalog", "f_table_schema", "f_table_name",
                                                            "f_geometry_column")
                                                    .anyMatch(cn -> dataIds.get(i).endsWith("geometry_columns/" + cn)))
                                            .toArray()
                                    : new int[0];
                            int[] typeIndexes = typeProtected
                                    ? IntStream.range(0, dataIds.size())
                                            .filter(i -> dataIds.get(i).endsWith("geometry_columns/type")).toArray()
                                    : new int[0];
                            if (Stream.of(catalogIndexes, dataIdIndexes, typeIndexes).anyMatch(arr -> arr.length > 0)) {
                                newOutboundDataOperation.setModified(true);
                            }
                        }
                        {
                            // process criterions
                            List<OutboundDataOperation.Criterion> criterions = newOutboundDataOperation.getCriterions();
                            int[] catalogIndexes = protectCatalogName
                                    ? IntStream.range(0, criterions.size()).filter(i -> criterions.get(i) != null)
                                            .filter(i -> criterions.get(i).getDataId()
                                                    .endsWith("geometry_columns/f_table_catalog"))
                                            .toArray()
                                    : new int[0];
                            int[] schemaIndexes = protectSchemaName
                                    ? IntStream.range(0, criterions.size()).filter(i -> criterions.get(i) != null)
                                            .filter(i -> criterions.get(i).getDataId()
                                                    .endsWith("geometry_columns/f_table_schema"))
                                            .toArray()
                                    : new int[0];
                            int[] tableIndexes = protectTableName
                                    ? IntStream.range(0, criterions.size())
                                            .filter(i -> criterions.get(i) != null).filter(i -> criterions.get(i)
                                                    .getDataId().endsWith("geometry_columns/f_table_name"))
                                            .toArray()
                                    : new int[0];
                            int[] columnIndexes = protectColumnName
                                    ? IntStream.range(0, criterions.size()).filter(i -> criterions.get(i) != null)
                                            .filter(i -> criterions.get(i).getDataId()
                                                    .endsWith("geometry_columns/f_geometry_column"))
                                            .toArray()
                                    : new int[0];
                            int[] typeIndexes = typeProtected
                                    ? IntStream.range(0, criterions.size())
                                            .filter(i -> criterions.get(i) != null).filter(i -> criterions.get(i)
                                                    .getDataId().endsWith("geometry_columns/type"))
                                            .toArray()
                                    : new int[0];
                            if (Stream.of(catalogIndexes, schemaIndexes, tableIndexes, columnIndexes, typeIndexes)
                                    .anyMatch(arr -> arr.length > 0)) {
                                if (Arrays.stream(catalogIndexes)
                                        .anyMatch(idx -> criterions.get(idx).getValue().equals(catalogName))
                                        || Arrays.stream(schemaIndexes)
                                                .anyMatch(idx -> criterions.get(idx).getValue().equals(schemaName))
                                        || Arrays.stream(tableIndexes)
                                                .anyMatch(idx -> criterions.get(idx).getValue().equals(tableName))
                                        || Arrays.stream(columnIndexes)
                                                .anyMatch(idx -> criterions.get(idx).getValue().equals(columnName))
                                        || Arrays.stream(typeIndexes)
                                                .anyMatch(idx -> criterions.get(idx).getValue().equals(clearType))) {
                                    Arrays.stream(catalogIndexes)
                                            .filter(idx -> criterions.get(idx).getValue().equals(catalogName))
                                            .forEach(idx -> criterions.get(idx)
                                                    .setValue(CString.valueOf(protectedCatalogName)));
                                    Arrays.stream(schemaIndexes)
                                            .filter(idx -> criterions.get(idx).getValue().equals(schemaName))
                                            .forEach(idx -> criterions.get(idx)
                                                    .setValue(CString.valueOf(protectedSchemaName)));
                                    Arrays.stream(tableIndexes)
                                            .filter(idx -> criterions.get(idx).getValue().equals(tableName))
                                            .forEach(idx -> criterions.get(idx)
                                                    .setValue(CString.valueOf(protectedTableName)));
                                    Arrays.stream(columnIndexes)
                                            .filter(idx -> criterions.get(idx).getValue().equals(columnName))
                                            .forEach(idx -> criterions.get(idx)
                                                    .setValue(CString.valueOf(protectedColumnName)));
                                    Arrays.stream(typeIndexes)
                                            .filter(idx -> criterions.get(idx).getValue().equals(clearType))
                                            .forEach(idx -> criterions.get(idx)
                                                    .setValue(CString.valueOf(protectedType)));
                                    newOutboundDataOperation.setModified(true);
                                }
                            }
                        }
                    });
                }
            }
        }
        String[] pgStatsDataids = Stream.of("schemaname", "tablename", "attname").map(cn -> "pg_stats/" + cn)
                .toArray(String[]::new);
        if (outboundDataOperation.getOperation() == Operation.READ && (outboundDataOperation.getCriterions().stream()
                .filter(c -> c != null).map(OutboundDataOperation.Criterion::getDataId)
                .anyMatch(id -> Stream.of(pgStatsDataids).anyMatch(cid -> id.endsWith(cid))))) {
            newOutboundDataOperations.stream().forEach(newOutboundDataOperation -> {
                // process criterions
                List<OutboundDataOperation.Criterion> criterions = newOutboundDataOperation.getCriterions();
                String clearDataId = "*/" + Stream.of(pgStatsDataids)
                        .map(cid -> criterions.stream().filter(c -> c.getDataId().endsWith(cid))
                                .map(OutboundDataOperation.Criterion::getValue).map(CString::toString).findFirst()
                                .orElse("*"))
                        .collect(Collectors.joining("/")).replaceFirst("/", ".");
                MetadataOperation metadataOperation = new MetadataOperation();
                metadataOperation.addDataId(CString.valueOf(clearDataId));
                List<Entry<CString, List<CString>>> metadata = newMetaDataOperation(ctx, metadataOperation)
                        .getMetadata();
                List<CString> protectedDataIds = !metadata.isEmpty() ? metadata.get(0).getValue() : null;
                String protectedDataId = protectedDataIds != null && !protectedDataIds.isEmpty()
                        ? protectedDataIds.get(0).toString() : null;
                boolean dataIdProtected = protectedDataId != null && !clearDataId.equals(protectedDataId);
                if (dataIdProtected) {
                    String schemaName = dataIdProtected ? getSchemaName(clearDataId) : "*";
                    String tableName = dataIdProtected ? getTableName(clearDataId) : "*";
                    String columnName = dataIdProtected ? getColumnName(clearDataId) : "*";
                    String protectedSchemaName = dataIdProtected ? getSchemaName(protectedDataId) : "*";
                    String protectedTableName = dataIdProtected ? getTableName(protectedDataId) : "*";
                    String protectedColumnName = dataIdProtected ? getColumnName(protectedDataId) : "*";
                    boolean protectSchemaName = !protectedSchemaName.equals("*") && !schemaName.equals("*")
                            && !schemaName.equals(protectedSchemaName);
                    boolean protectTableName = !protectedTableName.equals("*") && !tableName.equals("*")
                            && !tableName.equals(protectedTableName);
                    boolean protectColumnName = !protectedColumnName.equals("*") && !columnName.equals("*")
                            && !columnName.equals(protectedColumnName);
                    int[] schemaIndexes = protectSchemaName ? IntStream.range(0, criterions.size())
                            .filter(i -> criterions.get(i) != null)
                            .filter(i -> criterions.get(i).getDataId().endsWith("pg_stats/schemaname")).toArray()
                            : new int[0];
                    int[] tableIndexes = protectTableName
                            ? IntStream.range(0, criterions.size()).filter(i -> criterions.get(i) != null)
                                    .filter(i -> criterions.get(i).getDataId().endsWith("pg_stats/tablename")).toArray()
                            : new int[0];
                    int[] columnIndexes = protectColumnName
                            ? IntStream.range(0, criterions.size()).filter(i -> criterions.get(i) != null)
                                    .filter(i -> criterions.get(i).getDataId().endsWith("pg_stats/attname")).toArray()
                            : new int[0];
                    if (Stream.of(schemaIndexes, tableIndexes, columnIndexes).anyMatch(arr -> arr.length > 0)) {
                        if (Arrays.stream(schemaIndexes)
                                .anyMatch(idx -> criterions.get(idx).getValue().equals(schemaName))
                                || Arrays.stream(tableIndexes)
                                        .anyMatch(idx -> criterions.get(idx).getValue().equals(tableName))
                                || Arrays.stream(columnIndexes)
                                        .anyMatch(idx -> criterions.get(idx).getValue().equals(columnName))) {
                            Arrays.stream(schemaIndexes)
                                    .filter(idx -> criterions.get(idx).getValue().equals(schemaName))
                                    .forEach(idx -> criterions.get(idx).setValue(CString.valueOf(protectedSchemaName)));
                            Arrays.stream(tableIndexes).filter(idx -> criterions.get(idx).getValue().equals(tableName))
                                    .forEach(idx -> criterions.get(idx).setValue(CString.valueOf(protectedTableName)));
                            Arrays.stream(columnIndexes)
                                    .filter(idx -> criterions.get(idx).getValue().equals(columnName))
                                    .forEach(idx -> criterions.get(idx).setValue(CString.valueOf(protectedColumnName)));
                            newOutboundDataOperation.setModified(true);
                        }
                    }
                }
            });
        }
        return newOutboundDataOperations;
    }

    private boolean isFullyQualifiedDataId(String dataId) {
        Matcher matcher = FQ_DATA_ID_PATTERN.matcher(dataId);
        return matcher.matches();
    }

    private String getDatabaseName(String dataId) {
        Matcher matcher = FQ_DATA_ID_PATTERN.matcher(dataId);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    private String getSchemaName(String dataId) {
        Matcher matcher = FQ_DATA_ID_PATTERN.matcher(dataId);
        if (matcher.matches()) {
            return matcher.group(2);
        }
        return null;
    }

    private String getTableName(String dataId) {
        Matcher matcher = FQ_DATA_ID_PATTERN.matcher(dataId);
        if (matcher.matches()) {
            return matcher.group(3);
        }
        return null;
    }

    private String getColumnName(String dataId) {
        Matcher matcher = FQ_DATA_ID_PATTERN.matcher(dataId);
        if (matcher.matches()) {
            return matcher.group(4);
        }
        return null;
    }

    private InboundDataOperation newInboundDataOperation(ChannelHandlerContext ctx,
            List<InboundDataOperation> inboundDataOperations) {
        List<String> backendDatabaseNames = getBackendDatabaseNames(ctx);
        if (!backendDatabaseNames.isEmpty()) {
            // Modify data ids (replace back-end database name)
            inboundDataOperations.stream().forEach(inboundDataOperation -> {
                List<CString> dataIds = inboundDataOperation.getDataIds();
                String databaseName = getDatabaseName(ctx);
                IntStream.range(0, dataIds.size())
                        .forEach(i -> dataIds.set(i, modifyDatabaseName(dataIds.get(i), databaseName)));
            });
        }
        inboundDataOperations.stream().forEach(inboundDataOperation -> {
            if (inboundDataOperation.getOperation() == Operation.READ && inboundDataOperation.getDataIds().stream()
                    .anyMatch(id -> Stream.of("typname", "typelem").anyMatch(cn -> id.endsWith("pg_types/" + cn)))) {
                List<CString> dataIds = inboundDataOperation.getDataIds();
                int typnameIndex = IntStream.range(0, dataIds.size())
                        .filter(i -> dataIds.get(i).endsWith("pg_types/typname")).findFirst().orElse(-1);
                int typelemIndex = IntStream.range(0, dataIds.size())
                        .filter(i -> dataIds.get(i).endsWith("pg_types/typelem")).findFirst().orElse(-1);
                if (Stream.of(typnameIndex, typelemIndex).allMatch(idx -> idx != -1)) {
                    List<List<CString>> dataValues = inboundDataOperation.getDataValues();
                    Types backendTypes = getTypes(ctx, inboundDataOperation.getInvolvedCSP());
                    dataValues.forEach(row -> {
                        String typname = row.get(typnameIndex).toString();
                        String typelem = row.get(typelemIndex).toString();
                        backendTypes.setTypeOid(typname, Long.parseLong(typelem));
                    });
                }
            }
        });
        InboundDataOperation newInboundDataOperation = getProtocolService(ctx)
                .newInboundDataOperation(inboundDataOperations);
        if (!backendDatabaseNames.isEmpty()) {
            // Modify data ids (replace database name by the back-end database
            // name)
            int backend = newInboundDataOperation.getInvolvedCSP() == -1 ? getPreferredBackend(ctx)
                    : newInboundDataOperation.getInvolvedCSP();
            List<CString> dataIds = newInboundDataOperation.getDataIds();
            IntStream.range(0, dataIds.size()).forEach(
                    i -> dataIds.set(i, modifyDatabaseName(dataIds.get(i), backendDatabaseNames.get(backend))));
        }
        if (newInboundDataOperation.getOperation() == Operation.READ && newInboundDataOperation.getDataIds().stream()
                .anyMatch(id -> Stream.of("typname", "typelem").anyMatch(cn -> id.endsWith("pg_types/" + cn)))) {
            List<CString> dataIds = newInboundDataOperation.getDataIds();
            int typnameIndex = IntStream.range(0, dataIds.size())
                    .filter(i -> dataIds.get(i).endsWith("pg_types/typname")).findFirst().orElse(-1);
            int typelemIndex = IntStream.range(0, dataIds.size())
                    .filter(i -> dataIds.get(i).endsWith("pg_types/typelem")).findFirst().orElse(-1);
            if (Stream.of(typnameIndex, typelemIndex).allMatch(idx -> idx != -1)) {
                List<List<CString>> dataValues = newInboundDataOperation.getDataValues();
                Types types = getTypes(ctx, -1);
                dataValues.forEach(row -> {
                    String typname = row.get(typnameIndex).toString();
                    String typelem = row.get(typelemIndex).toString();
                    types.setTypeOid(typname, Long.parseLong(typelem));
                });
            }
        }
        if (newInboundDataOperation.getOperation() == Operation.READ && newInboundDataOperation.getDataIds().stream()
                .anyMatch(id -> Stream
                        .of("f_table_catalog", "f_table_schema", "f_table_name", "f_geometry_column", "srid", "type")
                        .anyMatch(cn -> id.endsWith("geometry_columns/" + cn)))) {
            Map<String, String> geometryObjectDefinition = getGeometryObjectDefinition(ctx);
            if (!geometryObjectDefinition.isEmpty()) {
                boolean catalogProtected = backendDatabaseNames.stream()
                        .anyMatch(dbn -> !dbn.equals(getDatabaseName(ctx)));
                String geometricDataId = geometryObjectDefinition.get(PgsqlConfiguration.GEOMETRIC_DATA_ID);
                List<CString> protectedGeometricDataIds = null;
                if (geometricDataId != null) {
                    MetadataOperation metadataOperation = new MetadataOperation();
                    metadataOperation.addDataId(CString.valueOf(geometricDataId));
                    List<Entry<CString, List<CString>>> metadata = newMetaDataOperation(ctx, metadataOperation)
                            .getMetadata();
                    protectedGeometricDataIds = !metadata.isEmpty() ? metadata.get(0).getValue() : null;
                }
                String protectedGeometricDataId = protectedGeometricDataIds != null
                        && !protectedGeometricDataIds.isEmpty() ? protectedGeometricDataIds.get(0).toString() : null;
                boolean dataIdProtected = geometricDataId != null && protectedGeometricDataId != null
                        && !geometricDataId.equals(protectedGeometricDataId);
                String clearType = geometryObjectDefinition.get(PgsqlConfiguration.GEOMETRIC_OBJECT_CLEAR_TYPE);
                String protectedType = geometryObjectDefinition.get(PgsqlConfiguration.GEOMETRIC_OBJECT_PROTECTED_TYPE);
                boolean typeProtected = !Objects.equals(clearType, protectedType);
                if (catalogProtected || dataIdProtected || typeProtected) {
                    String catalogName = dataIdProtected && !getDatabaseName(geometricDataId).equals("*")
                            ? getDatabaseName(geometricDataId) : catalogProtected ? getDatabaseName(ctx) : "*";
                    String schemaName = dataIdProtected ? getSchemaName(geometricDataId) : "*";
                    String tableName = dataIdProtected ? getTableName(geometricDataId) : "*";
                    String columnName = dataIdProtected ? getColumnName(geometricDataId) : "*";
                    String protectedCatalogName = dataIdProtected ? getDatabaseName(protectedGeometricDataId)
                            : catalogProtected ? backendDatabaseNames.get(0) : "*";
                    String protectedSchemaName = dataIdProtected ? getSchemaName(protectedGeometricDataId) : "*";
                    String protectedTableName = dataIdProtected ? getTableName(protectedGeometricDataId) : "*";
                    String protectedColumnName = dataIdProtected ? getColumnName(protectedGeometricDataId) : "*";
                    boolean unprotectCatalogName = !protectedCatalogName.equals("*") && !catalogName.equals("*")
                            && !catalogName.equals(protectedCatalogName);
                    boolean unprotectSchemaName = !protectedSchemaName.equals("*") && !schemaName.equals("*")
                            && !schemaName.equals(protectedSchemaName);
                    boolean unprotectTableName = !protectedTableName.equals("*") && !tableName.equals("*")
                            && !tableName.equals(protectedTableName);
                    boolean unprotectColumnName = !protectedColumnName.equals("*") && !columnName.equals("*")
                            && !columnName.equals(protectedColumnName);
                    List<CString> dataIds = newInboundDataOperation.getDataIds();
                    int[] catalogIndexes = unprotectCatalogName
                            ? IntStream.range(0, dataIds.size())
                                    .filter(i -> dataIds.get(i).endsWith("geometry_columns/f_table_catalog")).toArray()
                            : new int[0];
                    int[] schemaIndexes = unprotectSchemaName
                            ? IntStream.range(0, dataIds.size())
                                    .filter(i -> dataIds.get(i).endsWith("geometry_columns/f_table_schema")).toArray()
                            : new int[0];
                    int[] tableIndexes = unprotectTableName
                            ? IntStream.range(0, dataIds.size())
                                    .filter(i -> dataIds.get(i).endsWith("geometry_columns/f_table_name")).toArray()
                            : new int[0];
                    int[] columnIndexes = unprotectColumnName ? IntStream.range(0, dataIds.size())
                            .filter(i -> dataIds.get(i).endsWith("geometry_columns/f_geometry_column")).toArray()
                            : new int[0];
                    int[] typeIndexes = typeProtected
                            ? IntStream.range(0, dataIds.size())
                                    .filter(i -> dataIds.get(i).endsWith("geometry_columns/type")).toArray()
                            : new int[0];
                    if (Stream.of(catalogIndexes, schemaIndexes, tableIndexes, columnIndexes, typeIndexes)
                            .anyMatch(arr -> arr.length > 0)) {
                        List<List<CString>> dataValues = newInboundDataOperation.getDataValues();
                        long nbRows = dataValues.stream().filter(row -> {
                            return Arrays.stream(catalogIndexes)
                                    .anyMatch(idx -> row.get(idx).equals(protectedCatalogName))
                                    || Arrays.stream(schemaIndexes)
                                            .anyMatch(idx -> row.get(idx).equals(protectedSchemaName))
                                    || Arrays.stream(tableIndexes)
                                            .anyMatch(idx -> row.get(idx).equals(protectedTableName))
                                    || Arrays.stream(columnIndexes)
                                            .anyMatch(idx -> row.get(idx).equals(protectedColumnName))
                                    || Arrays.stream(typeIndexes).anyMatch(idx -> row.get(idx).equals(protectedType));
                        }).peek(row -> Arrays.stream(catalogIndexes)
                                .filter(idx -> row.get(idx).equals(protectedCatalogName))
                                .forEach(idx -> row.set(idx, CString.valueOf(catalogName))))
                                .peek(row -> Arrays.stream(schemaIndexes)
                                        .filter(idx -> row.get(idx).equals(protectedSchemaName))
                                        .forEach(idx -> row.set(idx, CString.valueOf(schemaName))))
                                .peek(row -> Arrays.stream(tableIndexes)
                                        .filter(idx -> row.get(idx).equals(protectedTableName))
                                        .forEach(idx -> row.set(idx, CString.valueOf(tableName))))
                                .peek(row -> Arrays.stream(columnIndexes)
                                        .filter(idx -> row.get(idx).equals(protectedColumnName))
                                        .forEach(idx -> row.set(idx, CString.valueOf(columnName))))
                                .peek(row -> Arrays.stream(typeIndexes)
                                        .filter(idx -> row.get(idx).equals(protectedType))
                                        .forEach(idx -> row.set(idx, CString.valueOf(clearType))))
                                .count();
                        newInboundDataOperation.setModified(nbRows > 0);
                    }
                }
                if (geometricDataId != null) {
                    List<CString> dataIds = newInboundDataOperation.getDataIds();
                    int catalogIndex = IntStream.range(0, dataIds.size())
                            .filter(i -> dataIds.get(i).endsWith("geometry_columns/f_table_catalog")).findFirst()
                            .orElse(-1);
                    int schemaIndex = IntStream.range(0, dataIds.size())
                            .filter(i -> dataIds.get(i).endsWith("geometry_columns/f_table_schema")).findFirst()
                            .orElse(-1);
                    int tableIndex = IntStream.range(0, dataIds.size())
                            .filter(i -> dataIds.get(i).endsWith("geometry_columns/f_table_name")).findFirst()
                            .orElse(-1);
                    int sridIndex = IntStream.range(0, dataIds.size())
                            .filter(i -> dataIds.get(i).endsWith("geometry_columns/srid")).findFirst().orElse(-1);
                    if (Stream.of(tableIndex, sridIndex).allMatch(idx -> idx != -1)) {
                        List<List<CString>> dataValues = newInboundDataOperation.getDataValues();
                        dataValues.forEach(row -> {
                            String catalog = catalogIndex != -1 ? row.get(catalogIndex).toString()
                                    : getDatabaseName(ctx);
                            String schema = schemaIndex != -1 ? row.get(schemaIndex).toString() : "public";
                            String table = row.get(tableIndex).toString();
                            String datasetSrid = String.format("%s/%s.%s", catalog, schema, table);
                            String srid = row.get(sridIndex).toString();
                            SQLDatabaseSchema databaseSchema = getDatabaseSchema(ctx);
                            databaseSchema.addDatasetSrid(datasetSrid, srid);
                        });
                    }
                }
            }
        }
        return newInboundDataOperation;
    }

    private String escapeRegex(String regex) {
        return regex.replace(".", "\\.").replace("[", "\\[").replace("]", "\\]").replace("(", "\\(").replace(")", "\\)")
                .replace("*", "[^/]*");
    }

    private String modifyDatabaseName(String protectedDataId, String databaseName) {
        return modifyDatabaseName(CString.valueOf(protectedDataId), databaseName).toString();
    }

    private CString modifyDatabaseName(CString protectedDataId, String databaseName) {
        int offset = protectedDataId.indexOf('/');
        if (offset != -1) {
            protectedDataId = CString.valueOf(databaseName).append(protectedDataId.substring(offset));
        }
        return protectedDataId;
    }

    private OutboundDataOperation extractSetOperation(ChannelHandlerContext ctx, SetStatement stmt)
            throws ParseException {
        OutboundDataOperation outboundDataOperation = new OutboundDataOperation();
        outboundDataOperation.setOperation(null);
        int numberOfBackends = getNumberOfBackends(ctx);
        outboundDataOperation
                .setInvolvedCSPs(IntStream.range(0, numberOfBackends).boxed().collect(Collectors.toList()));
        return outboundDataOperation;
    }

    private PgsqlStatement<SetStatement> modifySetStatement(ChannelHandlerContext ctx,
            PgsqlStatement<SetStatement> statement, OutboundDataOperation outboundDataOperation, boolean newStatement) {
        // Prepare statement
        SetStatement stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (SetStatement) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }

        return new PgsqlStatement<>(stmt, statement.getParameterTypes(), statement.getParameterFormats(),
                statement.getParameterValues(), statement.getResultFormats(), statement.getColumns());
    }

    private OutboundDataOperation extractStartTransactionOperation(ChannelHandlerContext ctx, StartTransaction stmt)
            throws ParseException {
        OutboundDataOperation outboundDataOperation = new OutboundDataOperation();
        outboundDataOperation.setOperation(null);
        int numberOfBackends = getNumberOfBackends(ctx);
        outboundDataOperation
                .setInvolvedCSPs(IntStream.range(0, numberOfBackends).boxed().collect(Collectors.toList()));
        return outboundDataOperation;
    }

    private PgsqlStatement<StartTransaction> modifyStartTransactionStatement(ChannelHandlerContext ctx,
            PgsqlStatement<StartTransaction> statement, OutboundDataOperation outboundDataOperation,
            boolean newStatement) {
        // Prepare statement
        StartTransaction stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (StartTransaction) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }

        return new PgsqlStatement<>(stmt, statement.getParameterTypes(), statement.getParameterFormats(),
                statement.getParameterValues(), statement.getResultFormats(), statement.getColumns());
    }

    private OutboundDataOperation extractCommitOperation(ChannelHandlerContext ctx, Commit stmt) throws ParseException {
        OutboundDataOperation outboundDataOperation = new OutboundDataOperation();
        outboundDataOperation.setOperation(null);
        int numberOfBackends = getNumberOfBackends(ctx);
        outboundDataOperation
                .setInvolvedCSPs(IntStream.range(0, numberOfBackends).boxed().collect(Collectors.toList()));
        return outboundDataOperation;
    }

    private PgsqlStatement<Commit> modifyCommitStatement(ChannelHandlerContext ctx, PgsqlStatement<Commit> statement,
            OutboundDataOperation outboundDataOperation, boolean newStatement) {
        // Prepare statement
        Commit stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (Commit) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }

        return new PgsqlStatement<>(stmt, statement.getParameterTypes(), statement.getParameterFormats(),
                statement.getParameterValues(), statement.getResultFormats(), statement.getColumns());
    }

    private OutboundDataOperation extractCreateTableOperation(ChannelHandlerContext ctx, CreateTable stmt)
            throws ParseException {
        OutboundDataOperation outboundDataOperation = new OutboundDataOperation();
        outboundDataOperation.setOperation(Operation.CREATE);
        outboundDataOperation.setUsingHeadOperation(true);
        // Extract dataset id
        String schemaId = getSchemaId(ctx, stmt.getTable());
        String datasetId = getTableId(ctx, stmt.getTable(), schemaId);
        // Save mapping between table and dataset id
        Map.Entry<Table, String> tableId = new SimpleEntry<>(stmt.getTable(), datasetId);
        outboundDataOperation.addAttribute("tableId", tableId);
        // Extract data ids
        List<CString> dataIds;
        dataIds = stmt.getColumnDefinitions().stream()
                // get column name
                .map(ColumnDefinition::getColumnName)
                // unquote string
                .map(StringUtilities::unquote)
                // build dataId
                .map(cn -> datasetId + cn)
                // transform to CString
                .map(CString::valueOf)
                // save as a list
                .collect(Collectors.toList());
        outboundDataOperation.setDataIds(dataIds);
        // Save mapping between column definitions and data ids
        List<Map.Entry<ColumnDefinition, String>> columnDefinitionIds = IntStream
                .range(0, stmt.getColumnDefinitions().size())
                .mapToObj(i -> new SimpleEntry<>(stmt.getColumnDefinitions().get(i), dataIds.get(i).toString()))
                .collect(Collectors.toList());
        outboundDataOperation.addAttribute("columnDefinitionIds", columnDefinitionIds);
        // Save the table definition for future insert
        SQLDatabaseSchema databaseSchema = getDatabaseSchema(ctx);
        databaseSchema.addDatasetDefinition(datasetId,
                dataIds.stream().map(CString::toString).collect(Collectors.toList()));
        return outboundDataOperation;
    }

    private PgsqlStatement<CreateTable> modifyCreateTableStatement(ChannelHandlerContext ctx,
            PgsqlStatement<CreateTable> statement, OutboundDataOperation outboundDataOperation, boolean newStatement) {
        // Prepare statement
        CreateTable stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (CreateTable) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }
        CreateTable createTable = stmt;

        int involvedBackend = outboundDataOperation.getInvolvedCSP() == -1 ? getPreferredBackend(ctx)
                : outboundDataOperation.getInvolvedCSP();

        // 1. Update clause column definitions
        // 1.1 Retrieve data ids associated to column definitions and table id
        // associated to the table
        @SuppressWarnings("unchecked")
        List<Map.Entry<ColumnDefinition, String>> columnDefinitionIds = (List<Map.Entry<ColumnDefinition, String>>) outboundDataOperation
                .getAttribute("columnDefinitionIds");
        List<String> dataIdPerColumnDefinition = columnDefinitionIds.stream().map(Map.Entry::getValue)
                .collect(Collectors.toList());

        // 1.2 Retrieve protected data ids
        List<String> newDataIds1 = outboundDataOperation.getDataIds().stream().map(CString::toString)
                .collect(Collectors.toList());

        // 1.3 Retrieve the mapping between clear and protected data ids
        // call metadata operation
        MetadataOperation metadataOperation1 = new MetadataOperation();
        List<CString> allDataIds1 = dataIdPerColumnDefinition.stream().map(CString::valueOf)
                .collect(Collectors.toList());
        metadataOperation1.setDataIds(allDataIds1);
        // mapping clear data ids to [protected data ids]:
        // [0 protected data ids] -> preserve original column name
        // [null protected data id for a csp] -> column name must be dropped
        // from the query
        // [non null protected data id for a csp] -> replace column name by 1
        // protected data id
        List<Map.Entry<CString, List<CString>>> metadata1 = newMetaDataOperation(ctx, metadataOperation1).getMetadata();
        // filter to retain metadata mapping only for the involved backend
        // [0 protected data id] -> preserve original column name
        // [1 null protected data id] -> column name must be dropped from the
        // query
        // [1 non null protected data id] -> replace column name by 1 protected
        // data id
        Map<String, List<String>> rawDataIdsMapping1 = metadata1.stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(),
                        e -> e.getValue().isEmpty() ? Collections.emptyList()
                                : Collections
                                        .singletonList(StringUtilities.toString(e.getValue().get(involvedBackend))),
                        (l1, l2) -> l1));
        // mapping original clear data ids to protected data ids:
        Map<String, List<String>> dataIdsMapping1 = allDataIds1.stream().distinct().map(CString::toString)
                .collect(Collectors.toMap(java.util.function.Function.identity(), id -> rawDataIdsMapping1.get(id)));
        // retrieve the mapping between clear table id and protected table id
        Map<String, List<String>> fqDataIdsMapping1 = dataIdsMapping1.entrySet().stream()
                .filter(e -> e.getKey().lastIndexOf('/') != -1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map.Entry<String, String> tableIdMapping = fqDataIdsMapping1.entrySet()
                .stream().filter(
                        e -> !e.getValue().isEmpty() && e.getValue().get(0) != null)
                .findAny()
                .map(e -> new SimpleEntry<>(
                        e.getKey().substring(0,
                                e.getKey()
                                        .lastIndexOf('/')),
                        e.getValue().get(0).substring(0, e.getValue().get(0).lastIndexOf('/'))))
                .orElse(fqDataIdsMapping1.keySet().stream().findAny()
                        .map(id -> new SimpleEntry<>(id.substring(0, id.lastIndexOf('/')),
                                id.substring(0, id.lastIndexOf('/'))))
                        .get());

        // 1.4 Replace or remove column definitions
        int nbColumnDefinitions = createTable.getColumnDefinitions().size();
        List<ColumnDefinition> newColumnDefinitions = new ArrayList<>(nbColumnDefinitions);
        for (int i = 0; i < nbColumnDefinitions; i++) {
            ColumnDefinition columnDefinition = createTable.getColumnDefinitions().get(i);
            String dataId = dataIdPerColumnDefinition.get(i);
            ColumnDefinition newColumnDefinition;
            if (dataId == null) {
                // no modification
                newColumnDefinition = columnDefinition;
            } else {
                // first filter data id mapping on key: retain protected data
                // id for the column definition
                List<String> columnDefinitionProtectedDataIds = dataIdsMapping1.entrySet().stream()
                        .filter(e -> dataId.contains(e.getKey())).flatMap(e -> e.getValue().stream())
                        .collect(Collectors.toList());
                if (columnDefinitionProtectedDataIds.isEmpty()) {
                    // no modification
                    newColumnDefinition = columnDefinition;
                } else {
                    // then filter data id mapping on value: retain protected
                    // data id that the data operation has returned
                    List<String> newColumnDefinitionDataIds = columnDefinitionProtectedDataIds.stream()
                            .filter(newDataIds1::contains).collect(Collectors.toList());
                    if (newColumnDefinitionDataIds.isEmpty()) {
                        // drop the column definition
                        newColumnDefinition = null;
                    } else {
                        // For the protected data id selected for the column
                        // definition, build a new column definition
                        String oldDataId = dataId;
                        String newDataId = newColumnDefinitionDataIds.get(0);
                        boolean same = newDataId.equals(oldDataId);
                        if (same) {
                            // no modification
                            newColumnDefinition = columnDefinition;
                        } else {
                            newColumnDefinition = buildColumnDefinition(columnDefinition, newDataId);
                        }
                    }
                }
            }
            if (newColumnDefinition != null) {
                newColumnDefinitions.add(newColumnDefinition);
            }
        }
        createTable.setColumnDefinitions(newColumnDefinitions);

        // 2. Update clause table
        // 2.1 Retrieve mapping between clear table ids and protected table ids
        // tableIdMapping is already resolved

        // 2.2 Replace or remove table
        Table table = createTable.getTable();
        Table newTable = null;
        String oldTableId = tableIdMapping.getKey();
        String newTableId = tableIdMapping.getValue();
        boolean same = newTableId.equals(oldTableId);
        if (same) {
            // no modification
            newTable = table;
        } else {
            newTable = buildTable(table, newTableId);
        }
        createTable.setTable(newTable);

        return new PgsqlStatement<>(stmt, statement.getParameterTypes(), statement.getParameterFormats(),
                statement.getParameterValues(), statement.getResultFormats(), statement.getColumns());
    }

    private ColumnDefinition buildColumnDefinition(ColumnDefinition columnDefinition, String newDataId) {
        String[] tokens = newDataId.split("/");
        assert tokens.length > 0;
        String newColumnName = tokens[tokens.length - 1];

        ColumnDefinition newColumnDefinition = new ColumnDefinition();
        newColumnDefinition.setColumnName(newColumnName);
        newColumnDefinition.setColDataType(columnDefinition.getColDataType());
        newColumnDefinition.setColumnSpecStrings(columnDefinition.getColumnSpecStrings());

        return newColumnDefinition;
    }

    private Table buildTable(Table table, String newTableId) {
        String[] tokens = newTableId.split("/");
        assert tokens.length > 0;
        String newDbName = tokens.length >= 2 ? tokens[tokens.length - 2] : null;
        String newTableName = tokens[tokens.length - 1];
        Database newDatabase = null;
        if (table.getDatabase() != null && !table.getDatabase().getFullyQualifiedName().isEmpty()
                && newDbName != null) {
            newDatabase = new Database(newDbName);
        }
        String[] tokens2 = newTableName.split("\\.");
        if (tokens2.length == 2) {
            newTableName = tokens2[1];
        }
        String newSchemaName = null;
        if (table.getSchemaName() != null) {
            newSchemaName = tokens2.length == 2 ? tokens2[0] : "public";
        }
        Table newTable = new Table(newDatabase, newSchemaName, newTableName);
        newTable.setAlias(table.getAlias());

        return newTable;
    }

    private OutboundDataOperation extractAlterTableOperation(ChannelHandlerContext ctx, Alter stmt,
            OutboundDataOperation outboundDataOperation, Operation operation) throws ParseException {
        // Extract dataset id
        String schemaId = getSchemaId(ctx, stmt.getTable());
        String datasetId = getTableId(ctx, stmt.getTable(), schemaId);
        if (outboundDataOperation == null || (operation != null && outboundDataOperation.getOperation() != operation)) {
            outboundDataOperation = new OutboundDataOperation();
            outboundDataOperation.setOperation(operation != null ? operation : Operation.UPDATE);
            outboundDataOperation.setUsingHeadOperation(true);
            // Save mapping between table and dataset id
            Map.Entry<Table, String> tableId = new SimpleEntry<>(stmt.getTable(), datasetId);
            outboundDataOperation.addAttribute("tableId", tableId);
        }
        // Extract data ids
        List<List<CString>> dataIdsPerAlterExpression;
        dataIdsPerAlterExpression = stmt.getAlterExpressions().stream()
                // get column names
                .map(this::extractColumnNames)
                .map(l -> l.stream()
                        // unquote string
                        .map(StringUtilities::unquote)
                        // build dataId
                        .map(cn -> datasetId + cn)
                        // transform to CString
                        .map(CString::valueOf)
                        // save as a list
                        .collect(Collectors.toList()))
                // save as a list
                .collect(Collectors.toList());
        List<CString> dataIds = dataIdsPerAlterExpression.stream().flatMap(List::stream).collect(Collectors.toList());
        dataIds.removeAll(outboundDataOperation.getDataIds());
        outboundDataOperation.addDataIds(dataIds);
        // Save mapping between alter expressions and data ids
        @SuppressWarnings("unchecked")
        List<Map.Entry<AlterExpression, List<String>>> alterExpressionIds = (List<Map.Entry<AlterExpression, List<String>>>) outboundDataOperation
                .getAttribute("alterExpressionIds");
        if (alterExpressionIds == null) {
            alterExpressionIds = IntStream.range(0, stmt.getAlterExpressions().size())
                    .mapToObj(i -> new SimpleEntry<>(stmt.getAlterExpressions().get(i), dataIdsPerAlterExpression.get(i)
                            .stream().map(CString::toString).collect(Collectors.toList())))
                    .collect(Collectors.toList());
            outboundDataOperation.addAttribute("alterExpressionIds", alterExpressionIds);
        } else {
            alterExpressionIds
                    .addAll(IntStream
                            .range(0,
                                    stmt.getAlterExpressions().size())
                            .mapToObj(
                                    i -> new SimpleEntry<>(stmt.getAlterExpressions().get(i), dataIdsPerAlterExpression
                                            .get(i).stream().map(CString::toString).collect(Collectors.toList())))
                            .collect(Collectors.toList()));
        }
        // Save the table definition for future insert
        SQLDatabaseSchema databaseSchema = getDatabaseSchema(ctx);
        List<String> datasetDefinition = databaseSchema.getDatasetDefinition(datasetId);
        if (datasetDefinition == null) {
            datasetDefinition = dataIds.stream().map(CString::toString).collect(Collectors.toList());
            databaseSchema.addDatasetDefinition(datasetId, datasetDefinition);
        }
        for (AlterExpression alterExpression : stmt.getAlterExpressions()) {
            String columnName = alterExpression.getColumnName();
            if (columnName != null) {
                String dataId = datasetId + columnName;
                if (alterExpression.getOperation() == AlterOperation.ADD) {
                    if (!datasetDefinition.contains(dataId)) {
                        datasetDefinition.add(dataId);
                    }
                } else if (alterExpression.getOperation() == AlterOperation.DROP) {
                    if (datasetDefinition.contains(dataId)) {
                        datasetDefinition.remove(dataId);
                    }
                }
            }
        }
        return outboundDataOperation;
    }

    private List<String> extractColumnNames(AlterExpression alterExpression) {
        String columnName = alterExpression.getColumnName();
        if (columnName != null) {
            return Collections.singletonList(columnName);
        }
        List<ColumnDataType> colDataTypes = alterExpression.getColDataTypeList();
        if (colDataTypes != null) {
            return colDataTypes.stream().map(ColumnDataType::getColumnName).collect(Collectors.toList());
        }
        List<String> pkColumns = alterExpression.getPkColumns();
        if (pkColumns != null) {
            return pkColumns;
        }
        List<String> ukColumns = alterExpression.getUkColumns();
        if (ukColumns != null) {
            return ukColumns;
        }
        List<String> fkColumns = alterExpression.getFkColumns();
        if (fkColumns != null) {
            return fkColumns;
        }
        Index index = alterExpression.getIndex();
        if (index != null) {
            return index.getColumnsNames();
        }
        return Collections.emptyList();
    }

    private PgsqlStatement<Alter> modifyAlterTableStatement(ChannelHandlerContext ctx, PgsqlStatement<Alter> statement,
            OutboundDataOperation outboundDataOperation, boolean newStatement) {
        // Prepare statement
        Alter stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (Alter) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }
        Alter alterTable = stmt;

        int involvedBackend = outboundDataOperation.getInvolvedCSP() == -1 ? getPreferredBackend(ctx)
                : outboundDataOperation.getInvolvedCSP();

        // 1. Update clause alter expressions
        // 1.1 Retrieve data ids associated to alter expressions and table id
        // associated to the table
        @SuppressWarnings("unchecked")
        List<Map.Entry<AlterExpression, List<String>>> alterExpressionIds = (List<Map.Entry<AlterExpression, List<String>>>) outboundDataOperation
                .getAttribute("alterExpressionIds");
        List<List<String>> dataIdsPerAlterExpression = alterExpressionIds.stream().map(Map.Entry::getValue)
                .collect(Collectors.toList());

        // 1.2 Retrieve protected data ids
        List<String> newDataIds1 = outboundDataOperation.getDataIds().stream().map(CString::toString)
                .collect(Collectors.toList());

        // 1.3 Retrieve the mapping between clear and protected data ids
        // call metadata operation
        MetadataOperation metadataOperation1 = new MetadataOperation();
        List<CString> allDataIds1 = dataIdsPerAlterExpression.stream().flatMap(List::stream).map(CString::valueOf)
                .collect(Collectors.toList());
        metadataOperation1.setDataIds(allDataIds1);
        // mapping clear data ids to [protected data ids]:
        // [0 protected data ids] -> preserve original column name
        // [null protected data id for a csp] -> column name must be dropped
        // from the query
        // [non null protected data id for a csp] -> replace column name by 1
        // protected data id
        List<Map.Entry<CString, List<CString>>> metadata1 = newMetaDataOperation(ctx, metadataOperation1).getMetadata();
        // filter to retain metadata mapping only for the involved backend
        // [0 protected data id] -> preserve original column name
        // [1 null protected data id] -> column name must be dropped from the
        // query
        // [1 non null protected data id] -> replace column name by 1 protected
        // data id
        Map<String, List<String>> rawDataIdsMapping1 = metadata1.stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(),
                        e -> e.getValue().isEmpty() ? Collections.emptyList()
                                : Collections
                                        .singletonList(StringUtilities.toString(e.getValue().get(involvedBackend))),
                        (l1, l2) -> l1));
        // mapping original clear data ids to protected data ids:
        Map<String, List<String>> dataIdsMapping1 = allDataIds1.stream().distinct().map(CString::toString)
                .collect(Collectors.toMap(java.util.function.Function.identity(), id -> rawDataIdsMapping1.get(id)));
        // retrieve the mapping between clear table id and protected table id
        Map<String, List<String>> fqDataIdsMapping1 = dataIdsMapping1.entrySet().stream()
                .filter(e -> e.getKey().lastIndexOf('/') != -1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map.Entry<String, String> tableIdMapping = fqDataIdsMapping1.entrySet()
                .stream().filter(
                        e -> !e.getValue().isEmpty() && e.getValue().get(0) != null)
                .findAny()
                .map(e -> new SimpleEntry<>(
                        e.getKey().substring(0,
                                e.getKey()
                                        .lastIndexOf('/')),
                        e.getValue().get(0).substring(0, e.getValue().get(0).lastIndexOf('/'))))
                .orElse(fqDataIdsMapping1.keySet().stream().findAny()
                        .map(id -> new SimpleEntry<>(id.substring(0, id.lastIndexOf('/')),
                                id.substring(0, id.lastIndexOf('/'))))
                        .get());

        // 1.4 Replace or remove alter expressions
        int nbAlterExpressions = alterTable.getAlterExpressions().size();
        List<AlterExpression> newAlterExpressions = new ArrayList<>(nbAlterExpressions);
        for (int i = 0; i < nbAlterExpressions; i++) {
            AlterExpression alterExpression = alterTable.getAlterExpressions().get(i);
            List<String> dataIds = dataIdsPerAlterExpression.get(i);
            AlterExpression newAlterExpression;
            if (dataIds.isEmpty()) {
                // no modification
                newAlterExpression = alterExpression;
            } else {
                // first filter data id mapping on key: retain protected data
                // ids for the alter expression
                List<String> alterExpressionProtectedDataIds = dataIdsMapping1.entrySet().stream()
                        .filter(e -> dataIds.contains(e.getKey())).flatMap(e -> e.getValue().stream())
                        .collect(Collectors.toList());
                if (alterExpressionProtectedDataIds.isEmpty()) {
                    // no modification
                    newAlterExpression = alterExpression;
                } else {
                    // then filter data id mapping on value: retain protected
                    // data ids that the data operation has returned
                    List<String> newAlterExpressionsDataIds = alterExpressionProtectedDataIds.stream()
                            .filter(newDataIds1::contains).collect(Collectors.toList());
                    if (newAlterExpressionsDataIds.isEmpty()) {
                        // drop the alter expression
                        newAlterExpression = null;
                    } else {
                        // For the protected data ids selected for the alter
                        // expression, build a new alter expression
                        List<String> oldDataIds = dataIds.stream().sorted().collect(Collectors.toList());
                        List<String> newDataIds = newAlterExpressionsDataIds.stream().sorted()
                                .collect(Collectors.toList());
                        boolean same = newDataIds.equals(oldDataIds);
                        if (same) {
                            // no modification
                            newAlterExpression = alterExpression;
                        } else {
                            newAlterExpression = buildAlterExpression(alterExpression, oldDataIds, newDataIds);
                        }
                    }
                }
            }
            if (newAlterExpression != null) {
                newAlterExpressions.add(newAlterExpression);
            }
        }
        alterTable.setAlterExpressions(newAlterExpressions);

        // 2. Update clause table
        // 2.1 Retrieve mapping between clear table ids and protected table ids
        // tableIdMapping is already resolved

        // 2.2 Replace or remove table
        Table table = alterTable.getTable();
        Table newTable = null;
        String oldTableId = tableIdMapping.getKey();
        String newTableId = tableIdMapping.getValue();
        boolean same = newTableId.equals(oldTableId);
        if (same) {
            // no modification
            newTable = table;
        } else {
            newTable = buildTable(table, newTableId);
        }
        alterTable.setTable(newTable);

        return new PgsqlStatement<>(stmt, statement.getParameterTypes(), statement.getParameterFormats(),
                statement.getParameterValues(), statement.getResultFormats(), statement.getColumns());
    }

    private AlterExpression buildAlterExpression(AlterExpression alterExpression, List<String> oldDataIds,
            List<String> newDataIds) {
        List<String> oldColumnNames = oldDataIds.stream().map(id -> id.split("/")).map(tk -> tk[tk.length - 1])
                .collect(Collectors.toList());
        List<String> newColumnNames = newDataIds.stream().map(id -> id.split("/")).map(tk -> tk[tk.length - 1])
                .collect(Collectors.toList());

        AlterExpression newAlterExpression = new AlterExpression();
        newAlterExpression.setOperation(alterExpression.getOperation());
        if (alterExpression.getColumnName() != null) {
            int idx = oldColumnNames.indexOf(alterExpression.getColumnName());
            if (idx != -1) {
                newAlterExpression.setColumnName(newColumnNames.get(idx));
            } else {
                newAlterExpression.setColumnName(alterExpression.getColumnName());
            }
        } else if (alterExpression.getColDataTypeList() != null) {
            alterExpression.getColDataTypeList().forEach(cdt -> {
                int idx = oldColumnNames.indexOf(cdt.getColumnName());
                if (idx != -1) {
                    newAlterExpression.addColDataType(newColumnNames.get(idx), cdt.getColDataType());
                } else {
                    newAlterExpression.addColDataType(cdt.getColumnName(), cdt.getColDataType());
                }
            });
        } else if (alterExpression.getConstraintName() != null) {
            alterExpression.setConstraintName(alterExpression.getConstraintName());
        } else if (alterExpression.getPkColumns() != null) {
            newAlterExpression.setPkColumns(alterExpression.getPkColumns().stream().map(pkc -> {
                int idx = oldColumnNames.indexOf(pkc);
                return idx != -1 ? newColumnNames.get(idx) : pkc;
            }).collect(Collectors.toList()));
        } else if (alterExpression.getUkColumns() != null) {
            newAlterExpression.setUkName(alterExpression.getUkName());
            newAlterExpression.setUkColumns(alterExpression.getUkColumns().stream().map(ukc -> {
                int idx = oldColumnNames.indexOf(ukc);
                return idx != -1 ? newColumnNames.get(idx) : ukc;
            }).collect(Collectors.toList()));
        } else if (alterExpression.getFkColumns() != null) {
            newAlterExpression.setFkColumns(alterExpression.getFkColumns().stream().map(fkc -> {
                int idx = oldColumnNames.indexOf(fkc);
                return idx != -1 ? newColumnNames.get(idx) : fkc;
            }).collect(Collectors.toList()));
            newAlterExpression.setFkSourceTable(alterExpression.getFkSourceTable());
            newAlterExpression.setFkSourceColumns(alterExpression.getFkSourceColumns());
            newAlterExpression.setOnDeleteCascade(alterExpression.isOnDeleteCascade());
            newAlterExpression.setOnDeleteRestrict(alterExpression.isOnDeleteRestrict());
            newAlterExpression.setOnDeleteSetNull(alterExpression.isOnDeleteSetNull());
        } else if (alterExpression.getIndex() != null) {
            Index oldIndex = alterExpression.getIndex();
            Index newIndex = new Index();
            newIndex.setType(oldIndex.getType());
            newIndex.setColumnsNames(oldIndex.getColumnsNames().stream().map(cn -> {
                int idx = oldColumnNames.indexOf(cn);
                return idx != -1 ? newColumnNames.get(idx) : cn;
            }).collect(Collectors.toList()));
            newIndex.setName(oldIndex.getName());
            newIndex.setIndexSpec(oldIndex.getIndexSpec());
            newAlterExpression.setIndex(newIndex);
        }
        return newAlterExpression;
    }

    private OutboundDataOperation extractCreateIndexOperation(ChannelHandlerContext ctx, CreateIndex stmt,
            OutboundDataOperation outboundDataOperation, Operation operation) throws ParseException {
        // Extract dataset id
        String schemaId = getSchemaId(ctx, stmt.getTable());
        String datasetId = getTableId(ctx, stmt.getTable(), schemaId);
        if (outboundDataOperation == null || (operation != null && outboundDataOperation.getOperation() != operation)) {
            outboundDataOperation = new OutboundDataOperation();
            outboundDataOperation.setOperation(operation != null ? operation : Operation.UPDATE);
            outboundDataOperation.setUsingHeadOperation(true);
            // Save mapping between table and dataset id
            Map.Entry<Table, String> tableId = new SimpleEntry<>(stmt.getTable(), datasetId);
            outboundDataOperation.addAttribute("tableId", tableId);
        }
        // Extract column names
        List<String> columnNames = stmt.getIndex().getColumnsNames();
        if (columnNames == null) {
            columnNames = Collections.emptyList();
        }
        // Extract data ids
        List<CString> dataIds = columnNames.stream()
                // unquote string
                .map(StringUtilities::unquote)
                // build dataId
                .map(cn -> datasetId + cn)
                // transform to CString
                .map(CString::valueOf)
                // save as a list
                .collect(Collectors.toList());
        dataIds.removeAll(outboundDataOperation.getDataIds());
        outboundDataOperation.addDataIds(dataIds);
        // Save mapping between method and data ids
        Map.Entry<String, List<String>> createIndexMethodIds = new SimpleEntry<>(stmt.getMethod(),
                dataIds.stream().map(CString::toString).collect(Collectors.toList()));
        outboundDataOperation.addAttribute("createIndexMethodIds", createIndexMethodIds);
        return outboundDataOperation;
    }

    private PgsqlStatement<CreateIndex> modifyCreateIndexStatement(ChannelHandlerContext ctx,
            PgsqlStatement<CreateIndex> statement, OutboundDataOperation outboundDataOperation, boolean newStatement) {
        // Prepare statement
        CreateIndex stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (CreateIndex) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }
        CreateIndex createIndex = stmt;

        int involvedBackend = outboundDataOperation.getInvolvedCSP() == -1 ? getPreferredBackend(ctx)
                : outboundDataOperation.getInvolvedCSP();

        // 1. Update clause index method
        // 1.1 Retrieve data ids associated to index method and table id
        // associated to the table
        @SuppressWarnings("unchecked")
        Map.Entry<String, List<String>> createIndexMethodIds = (Map.Entry<String, List<String>>) outboundDataOperation
                .getAttribute("createIndexMethodIds");
        List<String> dataIds = createIndexMethodIds.getValue();

        // 1.2 Retrieve protected data ids
        List<String> newDataIds1 = outboundDataOperation.getDataIds().stream().map(CString::toString)
                .collect(Collectors.toList());

        // 1.3 Retrieve the mapping between clear and protected data ids
        // call metadata operation
        MetadataOperation metadataOperation1 = new MetadataOperation();
        List<CString> allDataIds1 = dataIds.stream().map(CString::valueOf).collect(Collectors.toList());
        metadataOperation1.setDataIds(allDataIds1);
        // mapping clear data ids to [protected data ids]:
        // [0 protected data ids] -> preserve original column name
        // [null protected data id for a csp] -> column name must be dropped
        // from the query
        // [non null protected data id for a csp] -> replace column name by 1
        // protected data id
        List<Map.Entry<CString, List<CString>>> metadata1 = newMetaDataOperation(ctx, metadataOperation1).getMetadata();
        // filter to retain metadata mapping only for the involved backend
        // [0 protected data id] -> preserve original column name
        // [1 null protected data id] -> column name must be dropped from the
        // query
        // [1 non null protected data id] -> replace column name by 1 protected
        // data id
        Map<String, List<String>> rawDataIdsMapping1 = metadata1.stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(),
                        e -> e.getValue().isEmpty() ? Collections.emptyList()
                                : Collections
                                        .singletonList(StringUtilities.toString(e.getValue().get(involvedBackend))),
                        (l1, l2) -> l1));
        // mapping original clear data ids to protected data ids:
        Map<String, List<String>> dataIdsMapping1 = allDataIds1.stream().distinct().map(CString::toString)
                .collect(Collectors.toMap(java.util.function.Function.identity(), id -> rawDataIdsMapping1.get(id)));
        // retrieve the mapping between clear table id and protected table id
        Map<String, List<String>> fqDataIdsMapping1 = dataIdsMapping1.entrySet().stream()
                .filter(e -> e.getKey().lastIndexOf('/') != -1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map.Entry<String, String> tableIdMapping = fqDataIdsMapping1.entrySet()
                .stream().filter(
                        e -> !e.getValue().isEmpty() && e.getValue().get(0) != null)
                .findAny()
                .map(e -> new SimpleEntry<>(
                        e.getKey().substring(0,
                                e.getKey()
                                        .lastIndexOf('/')),
                        e.getValue().get(0).substring(0, e.getValue().get(0).lastIndexOf('/'))))
                .orElse(fqDataIdsMapping1.keySet().stream().findAny()
                        .map(id -> new SimpleEntry<>(id.substring(0, id.lastIndexOf('/')),
                                id.substring(0, id.lastIndexOf('/'))))
                        .get());

        // 1.4 Replace or remove columns
        int nbColumns = createIndex.getIndex().getColumnsNames() != null
                ? createIndex.getIndex().getColumnsNames().size() : 0;
        List<String> newColumnNames = new ArrayList<>(nbColumns);
        for (int i = 0; i < nbColumns; i++) {
            String columnName = createIndex.getIndex().getColumnsNames().get(i);
            String dataId = dataIds.get(i);
            String newColumnName;
            if (dataId == null) {
                // no modification
                newColumnName = columnName;
            } else {
                // first filter data id mapping on key: retain protected data
                // id for the column name
                List<String> columnProtectedDataIds = dataIdsMapping1.entrySet().stream()
                        .filter(e -> dataId.equals(e.getKey())).flatMap(e -> e.getValue().stream())
                        .collect(Collectors.toList());
                if (columnProtectedDataIds.isEmpty()) {
                    // no modification
                    newColumnName = columnName;
                } else {
                    // then filter data id mapping on value: retain protected
                    // data id that the data operation has returned
                    List<String> newColumnDataIds = columnProtectedDataIds.stream().filter(newDataIds1::contains)
                            .collect(Collectors.toList());
                    if (newColumnDataIds.isEmpty()) {
                        // drop the column name
                        newColumnName = null;
                    } else {
                        // For the protected data id selected for the column
                        // name,
                        // build a new column name
                        String oldDataId = dataId;
                        String newDataId = newColumnDataIds.get(0);
                        boolean same = newDataId.equals(oldDataId);
                        if (same) {
                            // no modification
                            newColumnName = columnName;
                        } else {
                            newColumnName = buildColumnName(columnName, newDataId);
                        }
                    }
                }
            }
            if (newColumnName != null) {
                newColumnNames.add(newColumnName);
            }
        }
        createIndex.getIndex().setColumnsNames(newColumnNames);

        // 2. Update clause table
        // 2.1 Retrieve mapping between clear table ids and protected table ids
        // tableIdMapping is already resolved

        // 2.2 Replace table
        Table table = createIndex.getTable();
        Table newTable = null;
        String oldTableId = tableIdMapping.getKey();
        String newTableId = tableIdMapping.getValue();
        boolean same = newTableId.equals(oldTableId);
        if (same) {
            // no modification
            newTable = table;
        } else {
            newTable = buildTable(table, newTableId);
        }
        createIndex.setTable(newTable);

        return new PgsqlStatement<>(stmt, statement.getParameterTypes(), statement.getParameterFormats(),
                statement.getParameterValues(), statement.getResultFormats(), statement.getColumns());
    }

    private String buildColumnName(String columnName, String newDataId) {
        String[] tokens = newDataId.split("/");
        assert tokens.length > 0;
        String newColumnName = tokens[tokens.length - 1];
        return newColumnName;
    }

    private OutboundDataOperation extractDropTableOperation(ChannelHandlerContext ctx, Drop stmt)
            throws ParseException {
        OutboundDataOperation outboundDataOperation = new OutboundDataOperation();
        outboundDataOperation.setOperation(Operation.DELETE);
        // Extract dataset id
        String schemaId = getSchemaId(ctx, stmt.getName());
        String datasetId = getTableId(ctx, stmt.getName(), schemaId);
        // Save mapping between table and dataset id
        Map.Entry<Table, String> tableId = new SimpleEntry<>(stmt.getName(), datasetId);
        outboundDataOperation.addAttribute("tableId", tableId);
        // Extract data ids
        List<CString> dataIds = Stream.of(CString.valueOf(datasetId + "*")).collect(Collectors.toList());
        outboundDataOperation.setDataIds(dataIds);
        return outboundDataOperation;
    }

    private PgsqlStatement<Drop> modifyDropTableStatement(ChannelHandlerContext ctx, PgsqlStatement<Drop> statement,
            OutboundDataOperation outboundDataOperation, boolean newStatement) {
        // Prepare statement
        Drop stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (Drop) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }
        Drop dropTable = stmt;

        // 1.1 Retrieve table id associated to the table
        @SuppressWarnings("unchecked")
        Map.Entry<Table, String> table2tableId = (Map.Entry<Table, String>) outboundDataOperation
                .getAttribute("tableId");
        String oldTableId = table2tableId.getValue();

        // 1.2 Retrieve protected data ids and protected table id
        List<String> newDataIds1 = outboundDataOperation.getDataIds().stream().map(CString::toString)
                .collect(Collectors.toList());
        String newTableId = newDataIds1.stream().filter(id -> id.lastIndexOf('/') != -1).findAny().get();
        newTableId = newTableId.substring(0, newTableId.lastIndexOf('/'));

        // 2. Update clause table
        // 2.1 Retrieve mapping between clear table ids and protected table ids
        // tableIdMapping is already resolved

        // 2.2 Replace or remove table
        Table table = dropTable.getName();
        Table newTable = null;
        boolean same = newTableId.equals(oldTableId);
        if (same) {
            // no modification
            newTable = table;
        } else {
            newTable = buildTable(table, newTableId);
        }
        dropTable.setName(newTable);

        return new PgsqlStatement<>(stmt, statement.getParameterTypes(), statement.getParameterFormats(),
                statement.getParameterValues(), statement.getResultFormats(), statement.getColumns());
    }

    private OutboundDataOperation extractInsertOperation(ChannelHandlerContext ctx, Insert stmt,
            List<ParameterValue> parameterValues, OutboundDataOperation outboundDataOperation) throws ParseException {
        if (outboundDataOperation == null) {
            outboundDataOperation = new OutboundDataOperation();
            outboundDataOperation.setOperation(Operation.CREATE);
            // Extract dataset id
            String schemaId = getSchemaId(ctx, stmt.getTable());
            String datasetId = getTableId(ctx, stmt.getTable(), schemaId);
            // Save mapping between table and dataset id
            Map.Entry<Table, String> tableId = new SimpleEntry<>(stmt.getTable(), datasetId);
            outboundDataOperation.addAttribute("tableId", tableId);
            // Extract data ids
            if (stmt.getColumns() == null) {
                SQLDatabaseSchema databaseSchema = getDatabaseSchema(ctx);
                List<String> datasetDefinition = databaseSchema.getDatasetDefinition(datasetId);
                List<CString> dataIds;
                if (datasetDefinition != null) {
                    dataIds = datasetDefinition.stream().map(CString::valueOf).collect(Collectors.toList());
                } else {
                    MetadataOperation metadataOperation = new MetadataOperation();
                    metadataOperation.addDataId(CString.valueOf(datasetId + "*"));
                    dataIds = newMetaDataOperation(ctx, metadataOperation).getDataIds();
                }
                outboundDataOperation.setDataIds(dataIds);
            } else {
                // get column name
                List<CString> dataIds = stmt.getColumns().stream().map(Column::getColumnName)
                        // unquote string
                        .map(StringUtilities::unquote)
                        // build dataId
                        .map(cn -> datasetId + cn)
                        // transform to CString
                        .map(CString::valueOf)
                        // build a list
                        .collect(Collectors.toList());
                // Save mapping between columns and data ids
                List<Map.Entry<Column, String>> columnIds = IntStream.range(0, stmt.getColumns().size())
                        .mapToObj(i -> new SimpleEntry<>(stmt.getColumns().get(i), dataIds.get(i).toString()))
                        .collect(Collectors.toList());
                outboundDataOperation.addAttribute("columnIds", columnIds);
                outboundDataOperation.setDataIds(dataIds);
            }
        } else if (outboundDataOperation.getAttribute("columnIds") == null && stmt.getColumns() != null) {
            // Extract dataset id
            String schemaId = getSchemaId(ctx, stmt.getTable());
            String datasetId = getTableId(ctx, stmt.getTable(), schemaId);
            // get column name
            List<CString> dataIds = stmt.getColumns().stream().map(Column::getColumnName)
                    // unquote string
                    .map(StringUtilities::unquote)
                    // build dataId
                    .map(cn -> datasetId + cn)
                    // transform to CString
                    .map(CString::valueOf)
                    // build a list
                    .collect(Collectors.toList());
            // Save mapping between columns and data ids
            List<Map.Entry<Column, String>> columnIds = IntStream.range(0, stmt.getColumns().size())
                    .mapToObj(i -> new SimpleEntry<>(stmt.getColumns().get(i), dataIds.get(i).toString()))
                    .collect(Collectors.toList());
            outboundDataOperation.addAttribute("columnIds", columnIds);
        }
        // Extract data values
        List<ExpressionList> rows = null;
        if (stmt.getItemsList() instanceof ExpressionList) {
            rows = Collections.singletonList((ExpressionList) stmt.getItemsList());
        } else if (stmt.getItemsList() instanceof MultiExpressionList) {
            rows = ((MultiExpressionList) stmt.getItemsList()).getExprList();
        }
        // TODO more complex insert statement
        // } else if (stmt.getItemsList() instanceof SubSelect) {
        // }
        if (rows != null) {
            for (ExpressionList row : rows) {
                List<CString> dataValue = row.getExpressions().stream()
                        // transform to string
                        .map(exp -> exp instanceof NullValue ? null : exp.toString())
                        // unquote value
                        .map(StringUtilities::unquote)
                        // transform to CString
                        .map(CString::valueOf).map(value -> {
                            if (parameterValues != null) {
                                // if value is a parameter id ($<index>)
                                if (value != null && value.charAt(0) == '$') {
                                    // get the parameter index
                                    int idx = Integer.parseInt(value.substring(1).toString()) - 1;
                                    // get the parameter value
                                    ParameterValue parameterValue = parameterValues.get(idx);
                                    // convert parameter value to string
                                    Types types = getTypes(ctx, -1);
                                    Type parameterType = types.getType(parameterValue.getType());
                                    value = convertToText(parameterType, -1, parameterValue.getFormat(),
                                            parameterValue.getValue());
                                }
                            }
                            return value;
                        })
                        // build a list
                        .collect(Collectors.toList());
                outboundDataOperation.addDataValue(dataValue);
            }
        }
        return outboundDataOperation;
    }

    private PgsqlStatement<Insert> modifyInsertStatement(ChannelHandlerContext ctx, PgsqlStatement<Insert> statement,
            OutboundDataOperation outboundDataOperation, boolean newStatement, int row) {
        // Prepare statement
        Insert stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (Insert) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }
        Insert insert = stmt;

        int involvedBackend = outboundDataOperation.getInvolvedCSP() == -1 ? getPreferredBackend(ctx)
                : outboundDataOperation.getInvolvedCSP();

        List<Integer> newDataIdIndexes;
        Map.Entry<String, String> tableIdMapping;
        // 1. Update columns in clause into (if any)
        if (insert.getColumns() != null) {
            // 1.1 Retrieve data ids associated to columns and table id
            // associated to the table
            @SuppressWarnings("unchecked")
            List<Map.Entry<Column, String>> columnIds = (List<Map.Entry<Column, String>>) outboundDataOperation
                    .getAttribute("columnIds");
            List<String> dataIdPerColumn = columnIds.stream().map(Map.Entry::getValue).collect(Collectors.toList());

            // 1.2 Retrieve protected data ids
            List<String> newDataIds1 = outboundDataOperation.getDataIds().stream().map(CString::toString)
                    .collect(Collectors.toList());

            // 1.3 Retrieve the mapping between clear and protected data ids
            // call metadata operation
            MetadataOperation metadataOperation1 = new MetadataOperation();
            List<CString> allDataIds1 = dataIdPerColumn.stream().map(CString::valueOf).collect(Collectors.toList());
            metadataOperation1.setDataIds(allDataIds1);
            // mapping clear data ids to [protected data ids]:
            // [0 protected data ids] -> preserve original column name
            // [null protected data id for a csp] -> column name must be dropped
            // from the query
            // [non null protected data id for a csp] -> replace column name by
            // 1 protected data id
            List<Map.Entry<CString, List<CString>>> metadata1 = newMetaDataOperation(ctx, metadataOperation1)
                    .getMetadata();
            // filter to retain metadata mapping only for the involved backend
            // [0 protected data id] -> preserve original column name
            // [1 null protected data id] -> column name must be dropped from
            // the query
            // [1 non null protected data id] -> replace column name by 1
            // protected data id
            Map<String, List<String>> rawDataIdsMapping1 = metadata1.stream()
                    .collect(
                            Collectors
                                    .toMap(e -> e.getKey().toString(),
                                            e -> e.getValue().isEmpty() ? Collections.emptyList()
                                                    : Collections.singletonList(StringUtilities
                                                            .toString(e.getValue().get(involvedBackend))),
                                            (l1, l2) -> l1));
            // mapping original clear data ids to protected data ids:
            Map<String, List<String>> dataIdsMapping1 = allDataIds1.stream().distinct().map(CString::toString).collect(
                    Collectors.toMap(java.util.function.Function.identity(), id -> rawDataIdsMapping1.get(id)));
            // retrieve the mapping between clear table id and protected table
            // id
            Map<String, List<String>> fqDataIdsMapping1 = dataIdsMapping1.entrySet().stream()
                    .filter(e -> e.getKey().lastIndexOf('/') != -1)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            tableIdMapping = fqDataIdsMapping1.entrySet()
                    .stream().filter(
                            e -> !e.getValue().isEmpty() && e.getValue().get(0) != null)
                    .findAny().map(
                            e -> new SimpleEntry<>(e.getKey().substring(0, e.getKey().lastIndexOf('/')),
                                    e.getValue().get(0)
                                            .substring(0,
                                                    e.getValue().get(0)
                                                            .lastIndexOf('/'))))
                    .orElse(fqDataIdsMapping1.keySet().stream().findAny()
                            .map(id -> new SimpleEntry<>(id.substring(0, id.lastIndexOf('/')),
                                    id.substring(0, id.lastIndexOf('/'))))
                            .get());

            // 1.4 Replace or remove columns
            int nbColumns = insert.getColumns().size();
            newDataIdIndexes = new ArrayList<>(nbColumns);
            List<Column> newColumns = new ArrayList<>(nbColumns);
            for (int i = 0; i < nbColumns; i++) {
                Column column = insert.getColumns().get(i);
                String dataId = dataIdPerColumn.get(i);
                Column newColumn;
                if (dataId == null) {
                    // no modification
                    newColumn = column;
                } else {
                    // first filter data id mapping on key: retain protected
                    // data
                    // id for the column
                    List<String> columnProtectedDataIds = dataIdsMapping1.entrySet().stream()
                            .filter(e -> dataId.contains(e.getKey())).flatMap(e -> e.getValue().stream())
                            .collect(Collectors.toList());
                    if (columnProtectedDataIds.isEmpty()) {
                        // no modification
                        newColumn = column;
                    } else {
                        // then filter data id mapping on value: retain
                        // protected
                        // data id that the data operation has returned
                        List<String> newColumnDataIds = columnProtectedDataIds.stream().filter(newDataIds1::contains)
                                .collect(Collectors.toList());
                        if (newColumnDataIds.isEmpty()) {
                            // drop the column
                            newColumn = null;
                        } else {
                            // For the protected data id selected for the
                            // column,
                            // build a new column
                            String oldDataId = dataId;
                            String newDataId = newColumnDataIds.get(0);
                            boolean same = newDataId.equals(oldDataId);
                            if (same) {
                                // no modification
                                newColumn = column;
                            } else {
                                newColumn = buildColumn(column, oldDataId, newDataId);
                            }
                        }
                    }
                }
                if (newColumn != null) {
                    newDataIdIndexes.add(newColumns.size());
                    newColumns.add(newColumn);
                } else {
                    newDataIdIndexes.add(-1);
                }
            }
            insert.setColumns(newColumns);
        } else {
            // 1.1 Retrieve table id associated to the table
            @SuppressWarnings("unchecked")
            Map.Entry<Table, String> table2tableId = (Map.Entry<Table, String>) outboundDataOperation
                    .getAttribute("tableId");
            String tableId = table2tableId.getValue();

            // 1.2 Retrieve protected data ids and protected table id
            List<String> newDataIds1 = outboundDataOperation.getDataIds().stream().map(CString::toString)
                    .collect(Collectors.toList());
            String newTableId = newDataIds1.stream().filter(id -> id.lastIndexOf('/') != -1).findAny().get();
            newTableId = newTableId.substring(0, newTableId.lastIndexOf('/'));

            // 1.3 Retrieve the mapping between clear and protected data ids
            SQLDatabaseSchema databaseSchema = getDatabaseSchema(ctx);
            List<String> datasetDefinition = databaseSchema.getDatasetDefinition(tableId);
            List<CString> allDataIds1;
            if (datasetDefinition != null) {
                allDataIds1 = datasetDefinition.stream().map(CString::valueOf).collect(Collectors.toList());
            } else {
                allDataIds1 = Collections.singletonList(CString.valueOf(tableId + "*"));
            }
            // call metadata operation
            MetadataOperation metadataOperation1 = new MetadataOperation();
            metadataOperation1.setDataIds(allDataIds1);
            // mapping clear data ids to [protected data ids]:
            // [0 protected data ids] -> preserve original column name
            // [null protected data id for a csp] -> column name must be dropped
            // from the query
            // [non null protected data id for a csp] -> replace column name by
            // 1 protected data id
            List<Map.Entry<CString, List<CString>>> metadata1 = newMetaDataOperation(ctx, metadataOperation1)
                    .getMetadata();
            allDataIds1 = metadata1.stream().map(Map.Entry::getKey).collect(Collectors.toList());
            // filter to retain metadata mapping only for the involved backend
            // [0 protected data id] -> preserve original column name
            // [1 null protected data id] -> column name must be dropped from
            // the query
            // [1 non null protected data id] -> replace column name by 1
            // protected data id
            Map<String, List<String>> rawDataIdsMapping1 = metadata1.stream()
                    .collect(
                            Collectors
                                    .toMap(e -> e.getKey().toString(),
                                            e -> e.getValue().isEmpty() ? Collections.emptyList()
                                                    : Collections.singletonList(StringUtilities
                                                            .toString(e.getValue().get(involvedBackend))),
                                            (l1, l2) -> l1));
            // mapping original clear data ids to protected data ids:
            Map<String, List<String>> dataIdsMapping1 = allDataIds1.stream().distinct().map(CString::toString).collect(
                    Collectors.toMap(java.util.function.Function.identity(), id -> rawDataIdsMapping1.get(id)));

            // retrieve the mapping between clear table id and protected table
            // id
            tableIdMapping = new SimpleEntry<>(tableId, newTableId);

            // 1.4 Replace or remove columns
            int nbColumns = -1;
            ItemsList itemsList = insert.getItemsList();
            if (itemsList instanceof ExpressionList) {
                nbColumns = ((ExpressionList) itemsList).getExpressions().size();
            } else if (itemsList instanceof MultiExpressionList) {
                nbColumns = ((MultiExpressionList) itemsList).getExprList().get(0).getExpressions().size();
            } else if (itemsList instanceof SubSelect) {
                // TODO more complex insert statement
            }
            newDataIdIndexes = nbColumns != -1 ? new ArrayList<>(nbColumns) : new ArrayList<>();
            for (int i = 0; i < nbColumns; i++) {
                String dataId = allDataIds1.get(i).toString();
                int newIndex;
                if (dataId == null) {
                    // no modification
                    newIndex = i;
                } else {
                    // first filter data id mapping on key: retain protected
                    // data
                    // id for the column
                    List<String> columnProtectedDataIds = dataIdsMapping1.entrySet().stream()
                            .filter(e -> dataId.contains(e.getKey())).flatMap(e -> e.getValue().stream())
                            .collect(Collectors.toList());
                    if (columnProtectedDataIds.isEmpty()) {
                        // no modification
                        newIndex = i;
                    } else {
                        // then filter data id mapping on value: retain
                        // protected
                        // data id that the data operation has returned
                        List<String> newColumnDataIds = columnProtectedDataIds.stream().filter(newDataIds1::contains)
                                .collect(Collectors.toList());
                        if (newColumnDataIds.isEmpty()) {
                            // drop the value
                            newIndex = -1;
                        } else {
                            newIndex = i;
                        }
                    }
                }
                newDataIdIndexes.add(newIndex);
            }
        }

        // 2. Update clause into
        // 2.1 Retrieve mapping between clear table id and protected table id
        // tableIdMapping is already resolved

        // 2.2 Replace or remove table
        Table table = insert.getTable();
        Table newTable = null;
        String oldTableId = tableIdMapping.getKey();
        String newTableId = tableIdMapping.getValue();
        boolean same = newTableId.equals(oldTableId);
        if (same) {
            // no modification
            newTable = table;
        } else {
            newTable = buildTable(table, newTableId);
        }
        insert.setTable(newTable);

        // 3. Update clause values
        ItemsList itemsList = insert.getItemsList();
        List<ExpressionList> expressionLists = null;
        if (itemsList instanceof ExpressionList) {
            expressionLists = Collections.singletonList((ExpressionList) itemsList);
        } else if (itemsList instanceof MultiExpressionList) {
            expressionLists = ((MultiExpressionList) itemsList).getExprList();
        } else if (itemsList instanceof SubSelect) {
            // TODO more complex insert statement
        }
        if (expressionLists != null) {
            List<ParameterValue> parameterValues = statement.getParameterValues();
            for (ExpressionList expressionList : expressionLists) {
                List<Expression> expressions = expressionList.getExpressions();
                List<CString> dataValues = outboundDataOperation.getDataValues().get(row++);
                List<Expression> newExpressions = new ArrayList<>(dataValues.size());
                for (int i = 0; i < expressions.size(); i++) {
                    int newDataIdIndex = newDataIdIndexes.get(i);
                    if (newDataIdIndex != -1) {
                        Expression expression = expressions.get(i);
                        String oldValue = expression instanceof StringValue ? ((StringValue) expression).getValue()
                                : expression instanceof NullValue ? null : expression.toString();
                        String newValue = dataValues.get(newDataIdIndex) != null
                                ? dataValues.get(newDataIdIndex).toString() : null;
                        same = newValue == null ? oldValue == null : newValue.equals(oldValue);
                        Expression newExpression;
                        if (same) {
                            // no modification
                            newExpression = expression;
                            if (oldValue != null && oldValue.charAt(0) == '$') {
                                // modify parameter
                                if (parameterValues != null) {
                                    // get the parameter index
                                    int paramIndex = Integer.parseInt(oldValue.substring(1)) - 1;
                                    // get the parameter value
                                    ParameterValue parameterValue = parameterValues.get(paramIndex);
                                    // convert string to ByteBuf
                                    Types backendTypes = getTypes(ctx, involvedBackend);
                                    Type parameterType = backendTypes.getType(parameterValue.getType());
                                    ByteBuf value = convertToByteBuf(parameterType, -1, parameterValue.getFormat(),
                                            dataValues.get(newDataIdIndex));
                                    // modify parameter value
                                    parameterValue.setValue(value);
                                }
                            }
                        } else {
                            newExpression = buildExpression(expression, oldValue, newValue);
                        }
                        newExpressions.add(newExpression);
                    }
                }
                expressionList.setExpressions(newExpressions);
            }
        }

        return new PgsqlStatement<>(stmt, statement.getParameterTypes(), statement.getParameterFormats(),
                statement.getParameterValues(), statement.getResultFormats(), statement.getColumns());
    }

    private Column buildColumn(Column column, String oldDataId, String newDataId) {
        String[] tokens = oldDataId.split("/");
        assert tokens.length > 0;
        String oldTableName = tokens.length >= 2 ? tokens[tokens.length - 2] : null;
        tokens = newDataId.split("/");
        assert tokens.length > 0;
        String newDbName = tokens.length >= 3 ? tokens[tokens.length - 3] : null;
        String newTableName = tokens.length >= 2 ? tokens[tokens.length - 2] : null;
        String newColumnName = tokens[tokens.length - 1];
        Table table = column.getTable();
        Table newTable = null;
        if (table != null && !table.getFullyQualifiedName().isEmpty() && newTableName != null) {
            Database newDatabase = null;
            String newSchemaName = null;
            // test table name is an alias
            boolean alias = !table.getFullyQualifiedName().equalsIgnoreCase(oldTableName);
            if (alias) {
                newTableName = table.getFullyQualifiedName();
            } else {
                if (table.getDatabase() != null && !table.getDatabase().getFullyQualifiedName().isEmpty()
                        && newDbName != null) {
                    newDatabase = new Database(newDbName);
                }
                String[] tokens2 = newTableName.split("\\.");
                if (tokens2.length == 2) {
                    newTableName = tokens2[1];
                }
                if (table.getSchemaName() != null) {
                    newSchemaName = tokens2.length == 2 ? tokens2[0] : "public";
                }
            }
            newTable = new Table(newDatabase, newSchemaName, newTableName);
            newTable.setAlias(table.getAlias());
        }
        Column newColumn = new Column(newTable, newColumnName, column.getIndex());
        return newColumn;
    }

    private Expression buildExpression(Expression expression, String oldValue, String newValue) {
        if (newValue == null) {
            return new NullValue();
        } else if (expression instanceof Column) {
            return buildColumn((Column) expression, oldValue, newValue);
        } else if (expression instanceof StringValue) {
            if (StringUtilities.hasQuote(((StringValue) expression).toString())
                    && !StringUtilities.hasQuote(newValue)) {
                newValue = StringUtilities.quote(newValue);
            } else if (StringUtilities.hasSingleQuote(((StringValue) expression).toString())
                    && !StringUtilities.hasSingleQuote(newValue)) {
                newValue = StringUtilities.singleQuote(newValue);
            } else if (((StringValue) expression).toString().startsWith("E'") && !newValue.startsWith("E'")) {
                newValue = "E'" + newValue + "'";
            }
        }
        return new StringValue(newValue);
    }

    private ModuleOperation extractSelectOperation(ChannelHandlerContext ctx, Select stmt,
            List<ParameterValue> parameterValues, OutboundDataOperation outboundDataOperation, Operation operation)
            throws ParseException {
        if (!(stmt.getSelectBody() instanceof PlainSelect)) {
            return null;
        }
        // Extract table ids
        PlainSelect select = (PlainSelect) stmt.getSelectBody();
        int capacity = (select.getFromItem() instanceof Table ? 1 : 0)
                + (select.getJoins() != null ? select.getJoins().size() : 0);
        List<String> schemaIds = new ArrayList<>(capacity);
        List<Map.Entry<Table, String>> tableIds = new ArrayList<>(capacity);
        if (select.getFromItem() instanceof Table) {
            Table table = (Table) select.getFromItem();
            // trackTableDefinition(ctx, table);
            String schemaId = getSchemaId(ctx, table);
            schemaIds.add(schemaId);
            String tableId = getTableId(ctx, table, schemaId);
            tableIds.add(new SimpleEntry<>(table, tableId));
            if (select.getJoins() != null) {
                for (Join join : select.getJoins()) {
                    if (join.getRightItem() instanceof Table) {
                        table = (Table) join.getRightItem();
                        schemaId = getSchemaId(ctx, table);
                        if (!schemaIds.contains(schemaId)) {
                            schemaIds.add(schemaId);
                        }
                        tableId = getTableId(ctx, table, schemaId);
                        tableIds.add(new SimpleEntry<>(table, tableId));
                    }
                }
            }
        } else {
            String schemaId = getSchemaId(ctx, null);
            schemaIds.add(schemaId);
            String tableId = getTableId(ctx, null, schemaId);
            tableIds.add(new SimpleEntry<>(null, tableId));
        }
        // Extract data ids
        List<Map.Entry<SelectItem, List<String>>> selectItemIds = new ArrayList<>(select.getSelectItems().size());
        boolean metadata = false;
        boolean retrieveProtectedData = false;
        if (operation == null) {
            operation = Operation.READ;
        }
        boolean usingHeadOperation = false;
        if (outboundDataOperation != null) {
            usingHeadOperation = outboundDataOperation.isUsingHeadOperation();
        }
        List<Integer> involvedBackends = null;
        for (Iterator<SelectItem> iter = select.getSelectItems().iterator(); iter.hasNext();) {
            SelectItem selectItem = iter.next();
            if (selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                if (selectExpressionItem.getExpression() instanceof Function) {
                    Function function = (Function) selectExpressionItem.getExpression();
                    if (FUNCTION_METADATA.equalsIgnoreCase(function.getName())) {
                        metadata = true;
                        if (function.isAllColumns()) {
                            MetadataOperation metadataOperation = new MetadataOperation();
                            tableIds.stream().map(Map.Entry::getValue)
                                    .forEach(id -> metadataOperation.addDataId(CString.valueOf(id + "*")));
                            List<CString> dataIds = newMetaDataOperation(ctx, metadataOperation).getDataIds();
                            selectItemIds.add(new SimpleEntry<>(selectItem,
                                    dataIds.stream().map(CString::toString).collect(Collectors.toList())));
                        } else if (function.getParameters() != null && function.getParameters().getExpressions() != null
                                && !function.getParameters().getExpressions().isEmpty()) {
                            List<String> dataIds = new ArrayList<>();
                            for (Expression parameter : function.getParameters().getExpressions()) {
                                if (parameter instanceof Column) {
                                    Column column = (Column) parameter;
                                    if (column.getTable() != null && column.getTable().getName() != null) {
                                        dataIds.addAll(schemaIds.stream().map(
                                                id -> id + column.getTable().getName() + "/" + column.getColumnName())
                                                .collect(Collectors.toList()));
                                    } else {
                                        dataIds.addAll(tableIds.stream().map(Map.Entry::getValue)
                                                .map(id -> id + column.getColumnName()).collect(Collectors.toList()));
                                    }
                                }
                            }
                            selectItemIds.add(new SimpleEntry<>(selectItem, dataIds));
                        } else {
                            // Error: expected at least one parameter (* or
                            // column name)
                            throw new ParseException(FUNCTION_METADATA
                                    + " function requires at least one parameter (* or column name(s))");
                        }
                        involvedBackends = Collections.emptyList();
                        break;
                    } else if (FUNCTION_PROTECTED.equalsIgnoreCase(function.getName())) {
                        retrieveProtectedData = true;
                        if (function.getParameters() != null && function.getParameters().getExpressions() != null
                                && !function.getParameters().getExpressions().isEmpty()) {
                            involvedBackends = new ArrayList<>(function.getParameters().getExpressions().size());
                            for (Expression parameter : function.getParameters().getExpressions()) {
                                if (parameter instanceof StringValue
                                        && ((StringValue) parameter).getValue().startsWith("csp")) {
                                    String s = ((StringValue) parameter).getValue();
                                    s = s.substring("csp".length());
                                    involvedBackends.add(Integer.parseInt(s) - 1);
                                } else {
                                    // Error: expected at least one parameter
                                    // (backend names)
                                    throw new ParseException(FUNCTION_PROTECTED
                                            + " function requires string parameters (backend names)");
                                }
                            }
                        } else {
                            // Error: expected at least one parameter (backend
                            // names)
                            throw new ParseException(
                                    FUNCTION_PROTECTED + " function requires at least one parameter (backend names)");
                        }
                        iter.remove();
                    } else if (FUNCTION_ADD_GEOMETRY_COLUMN.equalsIgnoreCase(function.getName())
                            && function.getParameters() != null && function.getParameters().getExpressions() != null) {
                        if (outboundDataOperation != null) {
                            operation = outboundDataOperation.getOperation();
                        }
                        if (operation == Operation.READ) {
                            operation = Operation.UPDATE;
                        }
                        usingHeadOperation = true;
                        String dataId = buildDataIdFromFunction(ctx, function.getParameters().getExpressions());
                        selectItemIds.add(new SimpleEntry<>(selectItem, Collections.singletonList(dataId)));
                        String datasetId = dataId.substring(0, dataId.lastIndexOf('/'));
                        String srid = getSridFromAddGeometryColumn(ctx, function.getParameters().getExpressions());
                        SQLDatabaseSchema databaseSchema = getDatabaseSchema(ctx);
                        databaseSchema.addDatasetSrid(datasetId, srid);
                    } else if (FUNCTION_ST_ESTIMATED_EXTENT.equalsIgnoreCase(function.getName())
                            && function.getParameters() != null && function.getParameters().getExpressions() != null) {
                        if (outboundDataOperation != null) {
                            operation = outboundDataOperation.getOperation();
                        }
                        String dataId = buildDataIdFromFunction(ctx, function.getParameters().getExpressions());
                        selectItemIds.add(new SimpleEntry<>(selectItem, Collections.singletonList(dataId)));
                    } else if (FUNCTION_HAS_COLUMN_PRIVILEGE.equalsIgnoreCase(function.getName())
                            && function.getParameters() != null && function.getParameters().getExpressions() != null) {
                        if (outboundDataOperation != null) {
                            operation = outboundDataOperation.getOperation();
                        }
                        String dataId = buildDataIdFromHasColumnPrivilege(ctx,
                                function.getParameters().getExpressions());
                        selectItemIds.add(new SimpleEntry<>(selectItem, Collections.singletonList(dataId)));
                    } else if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
                        List<String> allDataIds = function.getParameters().getExpressions().stream()
                                .flatMap(parameter -> {
                                    Stream<String> dataIds = null;
                                    if (parameter instanceof Column) {
                                        Column column = (Column) parameter;
                                        String columnName = StringUtilities.unquote(column.getName(false));
                                        if (column.getTable() == null || column.getTable().getName() == null) {
                                            dataIds = tableIds.stream().map(Map.Entry::getValue)
                                                    .map(id -> id + columnName);
                                        } else {
                                            String name = column.getTable().getName();
                                            String shortColumnName = columnName
                                                    .substring(columnName.lastIndexOf('.') + 1);
                                            dataIds = tableIds.stream()
                                                    .sorted(Comparator
                                                            .comparingInt(e -> e.getKey().getAlias() != null ? 0 : 1))
                                                    .filter(e -> {
                                                        Table table = e.getKey();
                                                        return name.equals(table.getName()) || (table.getAlias() != null
                                                                && name.equals(table.getAlias().getName()));
                                                    }).map(Map.Entry::getValue).map(id -> id + shortColumnName);
                                        }
                                    }
                                    if (dataIds == null) {
                                        dataIds = Stream.empty();
                                    }
                                    return dataIds;
                                }).collect(Collectors.toList());
                        if (allDataIds.isEmpty()) {
                            allDataIds = Collections.singletonList(toFullyQualifiedOutputName(selectItem));
                        }
                        selectItemIds.add(new SimpleEntry<>(selectItem, allDataIds));
                    } else {
                        selectItemIds.add(new SimpleEntry<>(selectItem,
                                Collections.singletonList(toFullyQualifiedOutputName(selectItem))));
                    }
                } else if (selectExpressionItem.getExpression() instanceof Column) {
                    // trackTableDefinition(ctx, ((Column)
                    // selectExpressionItem.getExpression()).getColumnName(),
                    // selectExpressionItem.getAlias() != null ?
                    // selectExpressionItem.getAlias().getName() : null);
                    Column column = (Column) selectExpressionItem.getExpression();
                    List<String> dataIds = extractDataIds(tableIds, column);
                    selectItemIds.add(new SimpleEntry<>(selectItem, dataIds));
                } else {
                    selectItemIds.add(new SimpleEntry<>(selectItem,
                            Collections.singletonList(toFullyQualifiedOutputName(selectItem))));
                }
            } else if (selectItem instanceof AllColumns) {
                // trackTableDefinition(ctx, "oid", null);
                // trackTableDefinition(ctx, "relname", null);
                String asterisk = ((AllColumns) selectItem).toString();
                selectItemIds.add(new SimpleEntry<>(selectItem, tableIds.stream().map(Map.Entry::getValue)
                        .map(id -> id + asterisk).collect(Collectors.toList())));
            } else if (selectItem instanceof AllTableColumns) {
                AllTableColumns allTableColumns = (AllTableColumns) selectItem;
                String name = allTableColumns.getTable().getName();
                selectItemIds.add(new SimpleEntry<>(selectItem,
                        Collections.singletonList(tableIds.stream().sorted(Comparator.comparingInt(e -> {
                            Table table = e.getKey();
                            return table.getAlias() != null ? 0 : 1;
                        })).filter(e -> {
                            Table table = e.getKey();
                            return name.equals(table.getName())
                                    || (table.getAlias() != null && name.equals(table.getAlias().getName()));
                        }).map(e -> e.getValue() + "*").findFirst().orElse(allTableColumns.toString()))));
            } else {
                // no other possibilities
                selectItemIds.add(new SimpleEntry<>(selectItem,
                        Collections.singletonList(toFullyQualifiedOutputName(selectItem))));
            }
        }
        // Extract criterions
        PgsqlColumnsFinder columnsFinder = new PgsqlColumnsFinder();
        columnsFinder.parse(select.getWhere());
        List<Map.Entry<Expression, List<Column>>> whereClause = columnsFinder.getExpressionsWithColumnsToColumns();
        List<Map.Entry<Expression, List<String>>> criteriaIds = whereClause
                .stream().map(e -> new SimpleEntry<>(e.getKey(), e.getValue().stream()
                        .map(c -> extractDataIds(tableIds, c)).flatMap(List::stream).collect(Collectors.toList())))
                .collect(Collectors.toList());
        ModuleOperation moduleOperation;
        if (metadata) {
            MetadataOperation metadataOperation = new MetadataOperation();
            metadataOperation.setDataIds(selectItemIds.stream().map(Map.Entry::getValue).flatMap(ids -> ids.stream())
                    .map(CString::valueOf).collect(Collectors.toList()));
            moduleOperation = metadataOperation;
            moduleOperation.setInvolvedCSPs(involvedBackends);
        } else {
            outboundDataOperation = new OutboundDataOperation();
            outboundDataOperation.setOperation(operation);
            outboundDataOperation.setUsingHeadOperation(usingHeadOperation);
            // Extract data ids from select items
            outboundDataOperation.setDataIds(selectItemIds.stream().map(Map.Entry::getValue).flatMap(List::stream)
                    .map(CString::valueOf).collect(Collectors.toList()));
            // Extract data values from select items (significant functions)
            outboundDataOperation.setDataValues(Collections.singletonList(selectItemIds.stream().flatMap(entry -> {
                SelectItem selectItem = entry.getKey();
                List<String> dataIds = entry.getValue();
                Stream<String> values;
                if (selectItem instanceof SelectExpressionItem
                        && ((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
                    Function function = (Function) ((SelectExpressionItem) selectItem).getExpression();
                    if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
                        values = dataIds.stream().map(dataId -> {
                            boolean significant = false;
                            if (isFullyQualifiedDataId(dataId)) {
                                Pattern dataIdPattern = Pattern.compile(
                                        String.format("(%s\\.?)?(%s\\.?)?(%s\\.?)?(%s)?", getDatabaseName(dataId),
                                                getSchemaName(dataId), getTableName(dataId), getColumnName(dataId)));
                                for (Expression parameter : function.getParameters().getExpressions()) {
                                    String value = parameter instanceof StringValue
                                            ? ((StringValue) parameter).getValue() : parameter.toString();
                                    Matcher matcher = dataIdPattern.matcher(StringUtilities.unquote(value));
                                    if (matcher.matches()) {
                                        significant = true;
                                        break;
                                    }
                                }
                            }
                            return significant ? function.toString() : null;
                        });
                    } else {
                        values = Stream.generate((Supplier<String>) () -> null).limit(dataIds.size());
                    }
                } else {
                    values = Stream.generate((Supplier<String>) () -> null).limit(dataIds.size());
                }
                return values;
            }).map(CString::valueOf).collect(Collectors.toList())));
            // Extract basic criteria from all criteria
            List<OutboundDataOperation.Criterion> criterions = extractSignificantCriterions(criteriaIds);
            outboundDataOperation.setCriterions(criterions);
            criterions.stream().filter(criterion -> criterion != null).forEach(criterion -> {
                CString value = criterion.getValue();
                if (value != null) {
                    // if value is a parameter id ($<index>)
                    if (value != null && value.charAt(0) == '$') {
                        // get the parameter index
                        int idx = Integer.parseInt(value.substring(1).toString()) - 1;
                        // get the parameter value
                        ParameterValue parameterValue = parameterValues.get(idx);
                        // convert parameter value to string
                        Types types = getTypes(ctx, -1);
                        Type parameterType = types.getType(parameterValue.getType());
                        value = convertToText(parameterType, -1, parameterValue.getFormat(), parameterValue.getValue());
                        criterion.setValue(value);
                    }
                }
            });
            outboundDataOperation.setInvolvedCSPs(involvedBackends);
            // Disable unprotecting process if needed
            if (retrieveProtectedData) {
                outboundDataOperation.setUnprotectingDataEnabled(false);
                outboundDataOperation.setModified(true);
            }
            outboundDataOperation.addAttribute("tableIds", tableIds);
            outboundDataOperation.addAttribute("selectItemIds", selectItemIds);
            outboundDataOperation.addAttribute("criteriaIds", criteriaIds);
            moduleOperation = outboundDataOperation;
        }
        return moduleOperation;
    }

    private String buildDataIdFromFunction(ChannelHandlerContext ctx, List<Expression> parameters) {
        StringBuilder sb = new StringBuilder();
        int nbSep = 0;
        for (Expression parameter : parameters) {
            if (!(parameter instanceof StringValue) || ((StringValue) parameter).getValue().matches("\\d+")) {
                break;
            }
            if (sb.length() > 0) {
                sb.append('/');
                ++nbSep;
            }
            sb.append(((StringValue) parameter).getValue());
        }
        if (nbSep == 3) {
            // case of dbname/schema/table/column
            int i = sb.indexOf("/", sb.indexOf("/") + 1);
            sb.setCharAt(i, '.');
        } else if (nbSep == 2) {
            // case of schema/table/column
            int i = sb.indexOf("/");
            sb.setCharAt(i, '.');
            sb.insert(0, getDatabaseId(ctx));
        } else if (nbSep == 1) {
            // case of table/column
            int i = sb.indexOf("/");
            String schema = identifySchema(sb.substring(0, i));
            sb.insert(0, '.');
            sb.insert(0, schema);
            sb.insert(0, getDatabaseId(ctx));
        }
        String dataId = sb.toString();
        return dataId;
    }

    private String getSridFromAddGeometryColumn(ChannelHandlerContext ctx, List<Expression> parameters) {
        for (Expression parameter : parameters) {
            if (parameter instanceof StringValue && ((StringValue) parameter).getValue().matches("\\d+")) {
                return ((StringValue) parameter).getValue();
            }
        }
        return null;
    }

    private String buildDataIdFromHasColumnPrivilege(ChannelHandlerContext ctx, List<Expression> parameters) {
        StringBuilder sb = new StringBuilder();
        int offset = parameters.size() == 3 ? 0 : 1;
        int nbSep = 0;
        for (int i = offset; i < offset + 2; i++) {
            Expression parameter = parameters.get(i);
            if (!(parameter instanceof StringValue)) {
                throw new IllegalArgumentException("Unexepected parameter type (expected a StringValue");
            }
            if (sb.length() > 0) {
                sb.append('/');
                ++nbSep;
            }
            String value = ((StringValue) parameter).getValue();
            if (StringUtilities.hasQuote(value)) {
                value = StringUtilities.unquote(value).replace("\".\"", ".");
            }
            if (value.indexOf('.') != -1) {
                ++nbSep;
            }
            sb.append(value);
        }
        if (nbSep == 2) {
            // case of schema.table/column
            sb.insert(0, getDatabaseId(ctx));
        } else if (nbSep == 1) {
            // case of table/column
            int i = sb.indexOf("/");
            String schema = identifySchema(sb.substring(0, i));
            sb.insert(0, '.');
            sb.insert(0, schema);
            sb.insert(0, getDatabaseId(ctx));
        }
        String dataId = sb.toString();
        return dataId;
    }

    private List<String> extractDataIds(List<Map.Entry<Table, String>> tableIds, Column column) {
        List<String> dataIds;
        String columnName = StringUtilities.unquote(column.getName(false));
        if (column.getTable() == null || column.getTable().getName() == null) {
            dataIds = tableIds.stream().map(Map.Entry::getValue).map(id -> id + columnName)
                    .collect(Collectors.toList());
        } else {
            String name = column.getTable().getName();
            String shortColumnName = columnName.substring(columnName.lastIndexOf('.') + 1);
            dataIds = Collections.singletonList(tableIds.stream()
                    .sorted(Comparator.comparingInt(e -> e.getKey().getAlias() != null ? 0 : 1)).filter(e -> {
                        Table table = e.getKey();
                        return name.equals(table.getName())
                                || (table.getAlias() != null && name.equals(table.getAlias().getName()));
                    }).map(Map.Entry::getValue).map(id -> id + shortColumnName).findFirst().orElse(columnName));
        }
        return dataIds;
    }

    private List<OutboundDataOperation.Criterion> extractSignificantCriterions(
            List<Map.Entry<Expression, List<String>>> criteriaIds) {
        List<OutboundDataOperation.Criterion> basicCriteria = criteriaIds.stream().map(e -> {
            OutboundDataOperation.Criterion criterion = null;
            if (e.getKey() instanceof BinaryExpression) {
                String dataId;
                boolean left;
                if (!e.getValue().isEmpty()) {
                    dataId = e.getValue().stream().filter(id -> id.contains("geometry_columns")).findFirst()
                            .orElse(e.getValue().get(0));
                    left = e.getValue().indexOf(dataId) == 0;
                } else {
                    dataId = e.getKey().toString();
                    left = true;
                }
                criterion = binaryExpressionToCriterion(dataId, left, (BinaryExpression) e.getKey());
            }
            return criterion;
        }).collect(Collectors.toList());
        return basicCriteria;
    }

    private OutboundDataOperation.Criterion binaryExpressionToCriterion(String dataId, boolean left,
            BinaryExpression expression) {
        Expression leftExpression = expression.getLeftExpression();
        Expression rightExpression = expression.getRightExpression();
        OutboundDataOperation.Criterion criterion = null;
        if (expression instanceof AndExpression
                && ((leftExpression instanceof Column && rightExpression instanceof Function)
                        || (rightExpression instanceof Column && leftExpression instanceof Function))) {
            Function function = (Function) (leftExpression instanceof Function ? leftExpression : rightExpression);
            if (function.getName().equalsIgnoreCase("st_makeenvelope") && function.getParameters() != null
                    && function.getParameters().getExpressions() != null) {
                criterion = new OutboundDataOperation.Criterion(CString.valueOf(dataId), CString.valueOf("area"),
                        CString.valueOf(
                                PlainSelect.getStringList(function.getParameters().getExpressions(), true, false)));
            }
        }
        if (criterion == null) {
            Expression expr = left ? rightExpression : leftExpression;
            String value = expr instanceof StringValue ? ((StringValue) expr).getValue() : expr.toString();
            String operator = expression.getStringExpression();
            criterion = new OutboundDataOperation.Criterion(CString.valueOf(dataId), CString.valueOf(operator),
                    CString.valueOf(value));
        }
        return criterion;
    }

    private void trackTableDefinition(ChannelHandlerContext ctx, Table table) {
        if (table.getName().equalsIgnoreCase("pg_class")) {
            SQLSession session = getSession(ctx);
            session.setTableDefinitionEnabled(true);
        }
    }

    private void trackTableDefinition(ChannelHandlerContext ctx, String columnName, String alias) {
        SQLSession session = getSession(ctx);
        if (session.isTableDefinitionEnabled()) {
            if (columnName.equalsIgnoreCase("oid") || columnName.equalsIgnoreCase("relname")) {
                session.addRowDescriptionFieldToTrack(alias != null ? alias : columnName, columnName);
            }
        }
    }

    private String getDatabaseId(ChannelHandlerContext ctx) {
        StringBuilder sb = new StringBuilder();
        String databaseName = getDatabaseName(ctx);
        if (databaseName != null) {
            sb.append(databaseName);
        } else {
            sb.append('*');
        }
        sb.append('/');
        String databaseId = sb.toString();
        return databaseId;
    }

    private String getSchemaId(ChannelHandlerContext ctx, Table table) {
        StringBuilder sb = new StringBuilder();
        if (table != null && table.getDatabase() != null) {
            String databaseName = StringUtilities.unquote(table.getDatabase().getDatabaseName());
            if (databaseName != null && !databaseName.isEmpty()) {
                sb.append(databaseName.toLowerCase());
            }
        }
        if (sb.length() == 0) {
            sb.append(getDatabaseId(ctx));
        }
        if (table != null) {
            String schemaName = StringUtilities.unquote(table.getSchemaName());
            // In case schema is not specified, try to identify it
            if (schemaName == null) {
                String tableName = StringUtilities.unquote(table.getName());
                schemaName = identifySchema(tableName);
            }
            if (schemaName != null) {
                sb.append(schemaName.toLowerCase()).append('.');
            }
        }
        String schemaId = sb.toString();
        return schemaId;
    }

    private String identifySchema(String tableName) {
        // TODO to identify the schema:
        // first: list and save tables (select * from pg_tables)
        // second: get and save the search_path variable (SHOW search_path)
        // finally: return the table according to the search_path
        // As workaround, return pg_catalog for table names that start with
        // 'pg_'
        return tableName.startsWith("pg_") ? "pg_catalog" : "public";
    }

    private String getTableId(ChannelHandlerContext ctx, Table table, String schemaId) {
        StringBuilder sb = new StringBuilder();
        if (table != null) {
            String tableName = StringUtilities.unquote(table.getName());
            sb.append(schemaId).append(tableName.toLowerCase());
        }
        if (sb.length() == 0) {
            sb.append(schemaId).append('*');
        }
        sb.append('/');
        String datasetId = sb.toString();
        return datasetId;
    }

    private static class PgsqlStatement<T extends Statement> {
        private final T statement;
        private final List<Long> parameterTypes;
        private final List<Short> parameterFormats;
        private final List<ParameterValue> parameterValues;
        private final List<Short> resultFormats;
        private final List<CString> columns;

        public PgsqlStatement(T statement) {
            this(statement, null, null, null, null, null);
        }

        public PgsqlStatement(T statement, List<Long> parameterTypes, List<Short> parameterFormats,
                List<ParameterValue> parameterValues, List<Short> resultFormats, List<CString> columns) {
            this.statement = statement;
            this.parameterTypes = parameterTypes;
            this.parameterFormats = parameterFormats;
            this.parameterValues = parameterValues;
            this.resultFormats = resultFormats;
            this.columns = columns;
        }

        public T getStatement() {
            return statement;
        }

        public List<Long> getParameterTypes() {
            return parameterTypes;
        }

        public List<Short> getParameterFormats() {
            return parameterFormats;
        }

        public List<ParameterValue> getParameterValues() {
            return parameterValues;
        }

        public List<Short> getResultFormats() {
            return resultFormats;
        }

        public List<CString> getColumns() {
            return columns;
        }

    }

    private PgsqlStatement<Select> modifySelectStatement(ChannelHandlerContext ctx, PgsqlStatement<Select> statement,
            OutboundDataOperation outboundDataOperation, boolean newStatement, List<ExpectedField> expectedFields) {
        // Prepare statement
        Select stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (Select) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }
        PlainSelect select = (PlainSelect) stmt.getSelectBody();

        int involvedBackend = outboundDataOperation.getInvolvedCSP() == -1 ? getPreferredBackend(ctx)
                : outboundDataOperation.getInvolvedCSP();

        // 1. Update clause select
        // 1.1 Retrieve data ids associated to columns and table id associated
        // to each table
        @SuppressWarnings("unchecked")
        List<Map.Entry<SelectItem, List<String>>> selectItemIds = (List<Map.Entry<SelectItem, List<String>>>) outboundDataOperation
                .getAttribute("selectItemIds");
        List<List<String>> dataIdsPerSelectItem = selectItemIds.stream().map(Map.Entry::getValue)
                .collect(Collectors.toList());
        @SuppressWarnings("unchecked")
        List<Map.Entry<Table, String>> tableIds = (List<Map.Entry<Table, String>>) outboundDataOperation
                .getAttribute("tableIds");

        // 1.2 Retrieve protected data ids
        List<String> newDataIds1 = outboundDataOperation.getDataIds().stream().map(CString::toString)
                .collect(Collectors.toList());
        List<String> newDataValues1 = outboundDataOperation.getDataValues().get(0).stream()
                .map(StringUtilities::toString).collect(Collectors.toList());

        // 1.3 Retrieve the mapping between clear and protected data ids
        // call metadata operation
        MetadataOperation metadataOperation1 = new MetadataOperation();
        List<CString> allDataIds1 = dataIdsPerSelectItem.stream().flatMap(List::stream).map(CString::valueOf)
                .collect(Collectors.toList());
        metadataOperation1.setDataIds(allDataIds1);
        // mapping clear data ids to [protected data ids]:
        // [0 protected data ids] -> preserve original column name
        // [null protected data id for a csp] -> column name must be dropped
        // from the query
        // [non null protected data id for a csp] -> replace column name by 1
        // protected data id
        List<Map.Entry<CString, List<CString>>> metadata1 = newMetaDataOperation(ctx, metadataOperation1).getMetadata();
        // filter to retain metadata mapping only for the involved backend
        // [0 protected data id] -> preserve original column name
        // [1 null protected data id] -> column name must be dropped from the
        // query
        // [1 non null protected data id] -> replace column name by 1 protected
        // data id
        Map<String, List<String>> rawDataIdsMapping1 = metadata1.stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(),
                        e -> e.getValue().isEmpty() ? Collections.emptyList()
                                : Collections
                                        .singletonList(StringUtilities.toString(e.getValue().get(involvedBackend))),
                        (l1, l2) -> l1));
        // mapping original clear data ids (including asterisks) to [protected
        // data ids]:
        // [0 protected data id] -> preserve original column name
        // [1 null protected data id] -> column name must be dropped from the
        // query
        // [1 or more non null protected data ids] -> replace column name by 1
        // or more protected data ids
        Map<String, List<String>> dataIdsMapping1 = allDataIds1.stream().distinct().map(CString::toString)
                .collect(Collectors.toMap(java.util.function.Function.identity(), id -> {
                    if (id.endsWith("*")) {
                        String prefix = id.substring(0, id.length() - 1);
                        return rawDataIdsMapping1.entrySet().stream().filter(e -> e.getKey().startsWith(prefix))
                                .flatMap(e -> e.getValue().stream()).collect(Collectors.toList());
                    } else {
                        return rawDataIdsMapping1.get(id);
                    }
                }));
        // replace protected data ids by the original asterisk (if any)
        dataIdsMapping1.entrySet().stream().filter(e -> e.getKey().endsWith("*")).forEach(e -> {
            List<String> values = e.getValue().stream()
                    .map(pid -> pid != null ? pid.substring(0, pid.lastIndexOf('/') + 1) + "*" : null).distinct()
                    .collect(Collectors.toList());
            if (values.size() > 1) {
                values = values.stream().filter(pid -> pid != null).collect(Collectors.toList());
            }
            e.setValue(values);
        });
        // retrieve the mapping between clear table ids and protected table ids
        Map<String, List<Map.Entry<String, List<String>>>> metadataPerTableId = dataIdsMapping1.entrySet().stream()
                .filter(e -> e.getKey().lastIndexOf('/') != -1)
                .collect(Collectors.groupingBy(e -> e.getKey().substring(0, e.getKey().lastIndexOf('/'))));
        Map<String, List<String>> tableIdsMapping = metadataPerTableId.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> e.getValue().stream().map(Map.Entry::getValue).flatMap(List::stream)
                                .map(id -> id != null ? id.substring(0, id.lastIndexOf('/')) : null).distinct()
                                .collect(Collectors.toList())));

        // 1.4 Replace or remove columns
        int nbSelectItems = select.getSelectItems().size();
        List<SelectItem> newSelectItems = new ArrayList<>(nbSelectItems);
        for (int i = 0; i < nbSelectItems; i++) {
            SelectItem selectItem = select.getSelectItems().get(i);
            List<String> dataIds = dataIdsPerSelectItem.get(i);
            SelectItem newSelectItem;
            List<String> newSelectItemDataIds = null;
            if (dataIds.isEmpty()) {
                // no modification
                newSelectItem = selectItem;
            } else {
                // first filter data id mapping on key: retain protected data
                // ids for the select item
                List<String> selectItemProtectedDataIds = dataIdsMapping1.entrySet().stream()
                        .filter(e -> dataIds.contains(e.getKey())).flatMap(e -> e.getValue().stream())
                        .collect(Collectors.toList());
                if (selectItemProtectedDataIds.isEmpty()) {
                    // no modification
                    newSelectItem = selectItem;
                    List<String> backendDatabaseNames = getBackendDatabaseNames(ctx);
                    if (!backendDatabaseNames.isEmpty()) {
                        // Modify data ids (replace database name by the
                        // back-end database
                        // name)
                        newSelectItemDataIds = dataIds.stream()
                                .map(id -> modifyDatabaseName(id, backendDatabaseNames.get(involvedBackend)))
                                .collect(Collectors.toList());
                    } else {
                        newSelectItemDataIds = dataIds;
                    }
                } else {
                    // then filter data id mapping on value: retain protected
                    // data ids that the data operation has returned
                    newSelectItemDataIds = selectItemProtectedDataIds.stream().filter(id -> {
                        return id != null && id.endsWith("*")
                                ? newDataIds1.stream()
                                        .anyMatch(ndid -> ndid.startsWith(id.substring(0, id.length() - 1)))
                                : newDataIds1.contains(id);
                    }).collect(Collectors.toList());
                    if (newSelectItemDataIds.isEmpty()) {
                        // drop the select item
                        newSelectItem = null;
                    } else {
                        // For each protected data ids selected for the select
                        // item, build a new select item
                        List<String> newSelectItemDataValues = selectItemProtectedDataIds.stream().filter(id -> {
                            return id != null && id.endsWith("*")
                                    ? newDataIds1.stream()
                                            .anyMatch(ndid -> ndid.startsWith(id.substring(0, id.length() - 1)))
                                    : newDataIds1.contains(id);
                        }).map(id -> {
                            return id != null && id.endsWith("*")
                                    ? IntStream.range(0, newDataIds1.size())
                                            .filter(j -> newDataIds1.get(j)
                                                    .startsWith(id.substring(0, id.length() - 1)))
                                            .findFirst().getAsInt()
                                    : newDataIds1.indexOf(id);
                        }).map(j -> newDataValues1.get(j)).collect(Collectors.toList());
                        boolean same = true;
                        for (int idx = 0; same && idx < dataIds.size(); idx++) {
                            String oldDataId = dataIds.get(idx);
                            String shortOldDataId = oldDataId.chars().filter(c -> c == '/').count() == 2
                                    ? oldDataId.substring(oldDataId.indexOf('/')) : oldDataId;
                            String newDataId = newSelectItemDataIds.get(idx);
                            String shortNewDataId = newDataId.chars().filter(c -> c == '/').count() == 2
                                    ? newDataId.substring(newDataId.indexOf('/')) : newDataId;
                            same = shortNewDataId.equals(shortOldDataId);
                            if (same && selectItem instanceof SelectExpressionItem
                                    && ((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
                                String newExpression = newSelectItemDataValues.get(idx);
                                if (newExpression != null) {
                                    String oldExpression = selectItem.toString();
                                    same = newExpression.equals(oldExpression);
                                }
                            }
                        }
                        if (same) {
                            // no modification
                            newSelectItem = selectItem;
                        } else {
                            newSelectItem = buildSelectItem(outboundDataOperation, selectItem, dataIds,
                                    newSelectItemDataIds, newSelectItemDataValues);
                        }
                    }
                }
            }
            if (newSelectItem != null) {
                newSelectItems.add(newSelectItem);
            }
            if (expectedFields != null) {
                // save mapping between expected clear field names and protected
                // field names
                String clearFieldName = toOutputName(selectItem.toString());
                ExpectedField expectedField = i < expectedFields.size() ? expectedFields.get(i) : null;
                if (expectedField == null) {
                    expectedField = new ExpectedField(clearFieldName);
                    expectedFields.add(expectedField);
                    if (outboundDataOperation.isUnprotectingDataEnabled() && selectItem instanceof SelectExpressionItem
                            && ((SelectExpressionItem) selectItem).getExpression() instanceof Function
                            && Stream.of(FUNCTIONS_WITH_SUPPORTED_RETURN_TYPE).noneMatch(fct -> fct.equalsIgnoreCase(
                                    ((Function) ((SelectExpressionItem) selectItem).getExpression()).getName()))) {
                        List<String> attributeNames = Stream.of(toFullyQualifiedOutputName(selectItem))
                                .collect(Collectors.toList());
                        expectedField.addAttributeNames(attributeNames);
                    } else {
                        expectedField.addAttributeNames(dataIds);
                    }
                }
                if (newSelectItem != null) {
                    if (newSelectItemDataIds == null) {
                        throw new IllegalStateException("unexpected");
                    }
                    if (expectedField.getBackendProtectedFields(involvedBackend) != null) {
                        throw new IllegalStateException("unexpected");
                    }
                    String protectedFieldName = toOutputName(newSelectItem.toString());
                    List<String> protectedAttributeNames;
                    List<Map.Entry<String, Integer>> attributeMapping;
                    if (outboundDataOperation.isUnprotectingDataEnabled()
                            && newSelectItem instanceof SelectExpressionItem
                            && ((SelectExpressionItem) newSelectItem).getExpression() instanceof Function
                            && Stream.of(FUNCTIONS_WITH_SUPPORTED_RETURN_TYPE).noneMatch(fct -> fct.equalsIgnoreCase(
                                    ((Function) ((SelectExpressionItem) newSelectItem).getExpression()).getName()))) {
                        String protectedAttributeName = toFullyQualifiedOutputName(newSelectItem);
                        protectedAttributeNames = Stream.of(protectedAttributeName).collect(Collectors.toList());
                        attributeMapping = Stream.of(new SimpleEntry<>(protectedAttributeName, 0))
                                .collect(Collectors.toList());
                    } else {
                        protectedAttributeNames = newSelectItemDataIds;
                        Map<String, String> reverseDataIdsMapping1 = dataIdsMapping1.entrySet().stream()
                                .filter(e -> dataIds.contains(e.getKey()))
                                .flatMap(e -> e.getValue().isEmpty()
                                        ? Stream.of(new SimpleEntry<>(
                                                protectedAttributeNames.get(dataIds.indexOf(e.getKey())), e.getKey()))
                                        : e.getValue().stream()
                                                .filter(pid -> pid != null && protectedAttributeNames.contains(pid))
                                                .map(pid -> new SimpleEntry<>(pid, e.getKey())))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                        int offset = 0;
                        attributeMapping = new ArrayList<>(newSelectItemDataIds.size());
                        for (String newDataId : newSelectItemDataIds) {
                            String dataId = reverseDataIdsMapping1.get(newDataId);
                            for (int j = offset; j < dataIds.size(); j++) {
                                if (dataIds.get(j).equals(dataId)) {
                                    attributeMapping.add(new SimpleEntry<>(newDataId, j));
                                    break;
                                }
                            }
                            offset++;
                        }
                    }
                    expectedField
                            .setBackendProtectedFields(
                                    involvedBackend, Stream
                                            .of(new ExpectedProtectedField(involvedBackend, protectedFieldName,
                                                    protectedAttributeNames, attributeMapping))
                                            .collect(Collectors.toList()));
                }
            }
        }
        select.setSelectItems(newSelectItems);

        // 2. Update clause from
        // 2.1 Retrieve table ids
        List<String> tableIdPerTable = tableIds.stream().map(Map.Entry::getValue)
                .map(v -> v.substring(0, v.length() - 1)).collect(Collectors.toList());

        // 2.2 Retrieve protected table ids
        List<String> newTableIds = newDataIds1.stream().filter(id -> id.lastIndexOf('/') != -1)
                .map(id -> id.substring(0, id.lastIndexOf('/'))).distinct().collect(Collectors.toList());

        // 2.3 Retrieve mapping between clear table ids and protected table ids
        // tableIdsMapping is already resolved

        // 2.4 Replace or remove tables
        List<Table> newTables;
        if (select.getFromItem() != null) {
            List<Join> froms = new ArrayList<>();
            Join join = new Join();
            join.setRightItem(select.getFromItem());
            froms.add(join);
            if (select.getJoins() != null) {
                froms.addAll(select.getJoins());
            }
            int nbFrom = froms.size();
            List<Join> newFroms = new ArrayList<>(nbFrom);
            for (int i = 0, j = 0; i < nbFrom; i++) {
                Join from = froms.get(i);
                if (!(from.getRightItem() instanceof Table)) {
                    // no modification
                    newFroms.add(from);
                } else {
                    String tableId = tableIdPerTable.get(j++);
                    // First filter table id mapping on key: retain clear table
                    // id for the table
                    // Then filter table id mapping on value: retain protected
                    // table ids that the data operation has returned
                    List<String> newJoinTableIds = tableIdsMapping.entrySet().stream()
                            .filter(e -> tableId.equals(e.getKey()))
                            .flatMap(e -> e.getValue().stream().filter(id -> newTableIds.contains(id)))
                            .collect(Collectors.toList());
                    if (newJoinTableIds.isEmpty()) {
                        // no modification
                        newFroms.add(from);
                    } else {
                        // For each protected table ids selected for the table,
                        // build a new from
                        newJoinTableIds.forEach(id -> newFroms.add(buildFrom(from, id)));
                    }
                }
            }
            newTables = newFroms.stream().map(Join::getRightItem).filter(item -> item instanceof Table)
                    .map(Table.class::cast).collect(Collectors.toList());
            select.setFromItem(newFroms.remove(0).getRightItem());
            select.setJoins(newFroms);
        } else {
            newTables = null;
        }

        // 3. Update clause where
        if (select.getWhere() != null) {
            // 3.1 Retrieve data ids associated to columns
            @SuppressWarnings("unchecked")
            List<Map.Entry<Expression, List<String>>> criteriaIds = (List<Map.Entry<Expression, List<String>>>) outboundDataOperation
                    .getAttribute("criteriaIds");
            List<List<String>> dataIdsPerCriteria = criteriaIds.stream().map(Map.Entry::getValue)
                    .collect(Collectors.toList());

            // 3.2 Retrieve protected criterions
            List<Map.Entry<OutboundDataOperation.Criterion, Boolean>> criterionsIds = outboundDataOperation
                    .getCriterions().stream().map(c -> new SimpleEntry<>(c, false)).collect(Collectors.toList());

            // 3.3 Retrieve the mapping between clear and protected data ids
            // call metadata operation
            MetadataOperation metadataOperation2 = new MetadataOperation();
            List<CString> allDataIds2 = dataIdsPerCriteria.stream().flatMap(List::stream).map(CString::valueOf)
                    .collect(Collectors.toList());
            metadataOperation2.setDataIds(allDataIds2);
            // mapping clear data ids to [protected data ids]:
            // [0 protected data ids] -> preserve original column name
            // [null protected data id for a csp] -> column name must be dropped
            // from the query
            // [non null protected data id for a csp] -> replace column name by
            // 1 protected data id
            List<Map.Entry<CString, List<CString>>> metadata2 = newMetaDataOperation(ctx, metadataOperation2)
                    .getMetadata();
            // filter to retain metadata mapping only for the involved backend
            // [0 protected data id] -> preserve original column name
            // [1 null protected data id] -> column name must be dropped from
            // the query
            // [1 non null protected data id] -> replace column name by 1
            // protected data id
            Map<String, List<String>> dataIdsMapping2 = metadata2.stream()
                    .collect(
                            Collectors
                                    .toMap(e -> e.getKey().toString(),
                                            e -> e.getValue().isEmpty() ? Collections.emptyList()
                                                    : Collections.singletonList(StringUtilities
                                                            .toString(e.getValue().get(involvedBackend))),
                                            (l1, l2) -> l1));

            // 1.4 Replace or remove columns
            Expression whereClause = select.getWhere();
            if (!newStatement) {
                // Duplicate SQL expression
                String expression = whereClause.toString();
                try {
                    whereClause = CCJSqlParserUtil.parseCondExpression(expression);
                } catch (JSQLParserException | TokenMgrError e) {
                    // Should not occur
                    LOGGER.error("Parsing error for {} : ", expression);
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Parsing error details:", e);
                    }
                }
            }
            PgsqlColumnsFinder columnsFinder = new PgsqlColumnsFinder();
            columnsFinder.parse(whereClause);
            List<Expression> expressions = columnsFinder.getExpressionsWithColumns();
            Map<Expression, Expression> expressionsToParents = columnsFinder.getExpressionsToParents();
            Expression newWhereClause = whereClause;
            for (int i = 0; i < expressions.size(); i++) {
                Expression expression = expressions.get(i);
                Expression parentExpression = expressionsToParents.get(expression);
                List<String> criteriaDataIds = dataIdsPerCriteria.get(i);
                Expression newExpression;
                if (criteriaDataIds.isEmpty()) {
                    // no modification
                    newExpression = expression;
                } else {
                    // first filter data id mapping on key: retain protected
                    // data
                    // ids for the criteria
                    List<String> criteriaProtectedDataIds = dataIdsMapping2.entrySet().stream()
                            .filter(e -> criteriaDataIds.contains(e.getKey())).flatMap(e -> e.getValue().stream())
                            .collect(Collectors.toList());
                    if (criteriaProtectedDataIds.isEmpty()) {
                        // no modification
                        newExpression = expression;
                    } else {
                        // then filter data id mapping on value: retain
                        // protected
                        // data ids that the data operation has returned for the
                        // criteria
                        List<Map.Entry<String, OutboundDataOperation.Criterion>> newCriterions = criteriaProtectedDataIds
                                .stream()
                                .filter(id -> criterionsIds.stream()
                                        .anyMatch(e -> !e.getValue() && e.getKey() != null
                                                && e.getKey().getDataId().equals(id)))
                                .map(id -> criterionsIds.stream()
                                        .filter(e -> !e.getValue() && e.getKey() != null
                                                && e.getKey().getDataId().equals(id))
                                        .findFirst().orElse(null))
                                .filter(e -> e != null).peek(e -> e.setValue(true)).map(Map.Entry::getKey)
                                .map(c -> new SimpleEntry<>(c.getDataId().toString(), c)).collect(Collectors.toList());
                        if (newCriterions.isEmpty()) {
                            // drop the expression
                            newExpression = null;
                        } else {
                            // For each protected data ids selected for the
                            // expression,
                            // build a new expression
                            List<String> shortOldDataIds = criteriaDataIds.stream()
                                    .map(cdid -> cdid.chars().filter(c -> c == '/').count() == 2
                                            ? cdid.substring(cdid.indexOf('/')) : cdid)
                                    .collect(Collectors.toList());
                            boolean same = true;
                            for (Map.Entry<String, Criterion> entry : newCriterions) {
                                String newDataId = entry.getKey();
                                String shortNewDataId = newDataId.chars().filter(c -> c == '/').count() == 2
                                        ? newDataId.substring(newDataId.indexOf('/')) : newDataId;
                                int idx = shortOldDataIds.indexOf(shortNewDataId);
                                same = idx != -1;
                                if (same && expression instanceof BinaryExpression) {
                                    Criterion criterion = entry.getValue();
                                    OutboundDataOperation.Criterion oldCriterion = binaryExpressionToCriterion(
                                            criteriaDataIds.get(idx), idx == 0, (BinaryExpression) expression);
                                    same = criterion.getValue().equals(oldCriterion.getValue());
                                }
                                if (!same) {
                                    break;
                                }
                            }
                            ;
                            if (same || !(expression instanceof BinaryExpression)) {
                                // no modification
                                newExpression = expression;
                            } else {
                                newExpression = buildBinaryExpression((BinaryExpression) expression,
                                        newCriterions.stream().map(Map.Entry::getValue).collect(Collectors.toList()));
                            }
                        }
                    }
                }
                while (newExpression != expression) {
                    Expression newParentExpression = modifyParentExpression(parentExpression, expression,
                            newExpression);
                    if (newParentExpression != parentExpression) {
                        expression = parentExpression;
                        newExpression = newParentExpression;
                        parentExpression = expressionsToParents.get(expression);
                    } else {
                        if (newWhereClause == expression) {
                            newWhereClause = newExpression;
                        }
                        expression = newExpression;
                    }
                }
            }
            select.setWhere(newWhereClause);
        }

        // 4. Update clause order by
        if (select.getOrderByElements() != null) {
            // 4.1 Retrieve data ids associated to columns
            Map<OrderByElement, List<String>> orderByElementIds = new LinkedHashMap<>(
                    select.getOrderByElements().size());
            for (OrderByElement orderByElement : select.getOrderByElements()) {
                if (orderByElement.getExpression() instanceof Column) {
                    Column column = (Column) orderByElement.getExpression();
                    if (column.getTable() == null || column.getTable().getName() == null) {
                        orderByElementIds.put(orderByElement, tableIds.stream().map(Map.Entry::getValue)
                                .map(id -> id + column.getColumnName()).collect(Collectors.toList()));
                    } else {
                        String name = column.getTable().getName();
                        orderByElementIds.put(orderByElement, Collections.singletonList(tableIds.stream().filter(e -> {
                            Table table = e.getKey();
                            return name.equals(table.getName())
                                    || (table.getAlias() != null && name.equals(table.getAlias().getName()));
                        }).map(e -> e.getValue() + column.getColumnName()).findFirst().orElse(column.getName(false))));
                    }
                } else {
                    orderByElementIds.put(orderByElement, Collections.emptyList());
                }
            }
            List<List<String>> dataIdsPerOrderByElement = orderByElementIds.values().stream()
                    .collect(Collectors.toList());

            // 4.2 Retrieve mapping between clear data ids and protected data
            // ids
            // call metadata operation
            MetadataOperation metadataOperation3 = new MetadataOperation();
            dataIdsPerOrderByElement.stream().flatMap(List::stream)
                    .forEach(id -> metadataOperation3.addDataId(CString.valueOf(id)));
            // mapping clear data ids to [protected data ids]:
            // [0 protected data ids] -> preserve original column name
            // [null protected data id for a csp] -> column name must be dropped
            // from the query
            // [non null protected data id for a csp] -> replace column name by
            // 1 protected data id
            List<Map.Entry<CString, List<CString>>> metadata3 = newMetaDataOperation(ctx, metadataOperation3)
                    .getMetadata();
            // filter to retain metadata mapping only for the involved backend
            // [0 protected data id] -> preserve original column name
            // [1 null protected data id] -> column name must be dropped from
            // the query
            // [1 non null protected data id] -> replace column name by 1
            // protected data id
            Map<String, List<String>> dataIdsMapping3 = metadata3.stream()
                    .collect(
                            Collectors
                                    .toMap(e -> e.getKey().toString(),
                                            e -> e.getValue().isEmpty() ? Collections.emptyList()
                                                    : Collections.singletonList(StringUtilities
                                                            .toString(e.getValue().get(involvedBackend))),
                                            (l1, l2) -> l1));

            // 4.3 Retrieve protected data ids
            List<String> newDataIds3 = dataIdsMapping3.values().stream().flatMap(List::stream).filter(id -> id != null)
                    .collect(Collectors.toList());

            // 4.4 Replace or remove columns
            int nbOrderByElement = select.getOrderByElements().size();
            List<OrderByElement> newOrderByElements = new ArrayList<>(nbOrderByElement);
            for (int i = 0; i < nbOrderByElement; i++) {
                OrderByElement orderByElement = select.getOrderByElements().get(i);
                List<String> dataIds = dataIdsPerOrderByElement.get(i);
                if (dataIds.isEmpty()) {
                    // no modification
                    newOrderByElements.add(orderByElement);
                } else {
                    // first filter data id mapping on key: retain protected
                    // data ids for the order by element
                    List<String> orderByElementProtectedDataIds = dataIdsMapping3.entrySet().stream()
                            .filter(e -> dataIds.contains(e.getKey())).flatMap(e -> e.getValue().stream())
                            .collect(Collectors.toList());
                    if (orderByElementProtectedDataIds.isEmpty()) {
                        // no modification
                        newOrderByElements.add(orderByElement);
                    } else {
                        // then filter data id mapping on value: retain
                        // protected data ids that the data operation has
                        // returned
                        List<String> newOrderByElementDataIds = orderByElementProtectedDataIds.stream()
                                .filter(id -> newDataIds3.contains(id)).collect(Collectors.toList());
                        if (newOrderByElementDataIds.isEmpty()) {
                            // drop the order by element
                        } else {
                            // For each protected data ids selected for the
                            // order by element, build a new order by element
                            newOrderByElementDataIds.forEach(
                                    id -> newOrderByElements.add(buildOrderByElement(orderByElement, id, newTables)));
                        }
                    }
                }
            }
            select.setOrderByElements(newOrderByElements.isEmpty() ? null : newOrderByElements);
        }

        return new PgsqlStatement<>(stmt, statement.getParameterTypes(), statement.getParameterFormats(),
                statement.getParameterValues(), statement.getResultFormats(), statement.getColumns());
    }

    private SelectItem buildSelectItem(DataOperation dataOperation, SelectItem selectItem, List<String> oldDataIds,
            List<String> newDataIds, List<String> newExpressions) {
        SelectItem newSelectItem = selectItem;
        if (selectItem instanceof SelectExpressionItem
                && ((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
            // consider all expressions are equals (this is one expression for
            // several data ids)
            newSelectItem = buildFunctionItem(selectItem, newExpressions.get(0));
        } else {
            // consider all data ids are equals (a column cannot have more than
            // one name)
            newSelectItem = buildColumnItem(selectItem, oldDataIds.get(0), newDataIds.get(0));
        }
        return newSelectItem;
    }

    private SelectItem buildFunctionItem(SelectItem functionItem, String newExpression) {
        Function function = (Function) ((SelectExpressionItem) functionItem).getExpression();
        Function newFunction = function;
        CCJSqlParser parser = new CCJSqlParser(new StringReader(newExpression));
        try {
            newFunction = parser.Function();
        } catch (Exception e) {
            LOGGER.error("Parsing error for {} : ", newExpression, e);
        }
        SelectExpressionItem newSelectExpressionItem = new SelectExpressionItem(newFunction);
        SelectExpressionItem selectExpressionItem = (SelectExpressionItem) functionItem;
        newSelectExpressionItem.setAlias(selectExpressionItem.getAlias());
        SelectItem newFunctionItem = newSelectExpressionItem;
        return newFunctionItem;
    }

    private SelectItem buildColumnItem(SelectItem columnItem, String oldDataId, String newDataId) {
        String[] tokens = oldDataId.split("/");
        assert tokens.length > 0;
        String oldTableName = tokens.length >= 2 ? tokens[tokens.length - 2] : null;
        tokens = newDataId.split("/");
        assert tokens.length > 0;
        String newDbName = tokens.length >= 3 ? tokens[tokens.length - 3] : null;
        String newTableName = tokens.length >= 2 ? tokens[tokens.length - 2] : null;
        String newColumnName = tokens[tokens.length - 1];
        Table table = null;
        if (columnItem instanceof SelectExpressionItem
                && ((SelectExpressionItem) columnItem).getExpression() instanceof Column) {
            table = ((Column) ((SelectExpressionItem) columnItem).getExpression()).getTable();
        } else if (columnItem instanceof AllTableColumns) {
            table = ((AllTableColumns) columnItem).getTable();
        }
        Table newTable = null;
        if (table != null && !table.getFullyQualifiedName().isEmpty() && newTableName != null) {
            Database newDatabase = null;
            String newSchemaName = null;
            // test table name is an alias
            boolean alias = !table.getFullyQualifiedName().equalsIgnoreCase(oldTableName);
            if (alias) {
                newTableName = table.getFullyQualifiedName();
            } else {
                if (table.getDatabase() != null && !table.getDatabase().getFullyQualifiedName().isEmpty()
                        && newDbName != null) {
                    newDatabase = new Database(newDbName);
                }
                String[] tokens2 = newTableName.split("\\.");
                if (tokens2.length == 2) {
                    newTableName = tokens2[1];
                }
                if (table.getSchemaName() != null) {
                    newSchemaName = tokens2.length == 2 ? tokens2[0] : "public";
                }
            }
            newTable = new Table(newDatabase, newSchemaName, newTableName);
            newTable.setAlias(table.getAlias());
        }
        SelectItem newColumnItem;
        if (newColumnName.equals("*")) {
            if (newTable != null) {
                newColumnItem = new AllTableColumns(newTable);
            } else {
                newColumnItem = new AllColumns();
            }
        } else {
            Column newColumn = new Column(newTable, newColumnName);
            SelectExpressionItem newSelectExpressionItem = new SelectExpressionItem(newColumn);
            if (columnItem instanceof SelectExpressionItem) {
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) columnItem;
                newSelectExpressionItem.setAlias(selectExpressionItem.getAlias());
                Column column = (Column) selectExpressionItem.getExpression();
                newColumn.setIndex(column.getIndex());
            }
            newColumnItem = newSelectExpressionItem;
        }
        return newColumnItem;
    }

    private String toFullyQualifiedOutputName(SelectItem selectItem) {
        String fqOutputName = null;
        if (selectItem instanceof SelectExpressionItem) {
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
            if (selectExpressionItem.getExpression() instanceof Function) {
                Function function = (Function) selectExpressionItem.getExpression();
                fqOutputName = function.getName() + "()";
                if (function.getAlias() != null) {
                    fqOutputName += " " + function.getAlias();
                }
            } else if (selectExpressionItem.getExpression() instanceof Column) {
                Column column = (Column) selectExpressionItem.getExpression();
                fqOutputName = column.getName(false);
            }
            if (selectExpressionItem.getAlias() != null && fqOutputName != null) {
                fqOutputName += " " + selectExpressionItem.getAlias();
            }
        }
        if (fqOutputName == null) {
            fqOutputName = selectItem.toString();
        }
        return fqOutputName;
    }

    private Join buildFrom(Join from, String tableId) {
        String[] tokens = tableId.split("/");
        assert tokens.length > 0;
        String newDbName = tokens.length >= 2 ? tokens[tokens.length - 2] : null;
        String newTableName = tokens[tokens.length - 1];
        Table table = (Table) from.getRightItem();
        Database newDatabase = null;
        if (table.getDatabase() != null && !table.getDatabase().getFullyQualifiedName().isEmpty()
                && newDbName != null) {
            newDatabase = new Database(newDbName);
        }
        String[] tokens2 = newTableName.split("\\.");
        if (tokens2.length == 2) {
            newTableName = tokens2[1];
        }
        String newSchemaName = null;
        if (table.getSchemaName() != null) {
            newSchemaName = tokens2.length == 2 ? tokens2[0] : "public";
        }
        Table newTable = new Table(newDatabase, newSchemaName, newTableName);
        newTable.setAlias(table.getAlias());
        Join newFrom = new Join();
        newFrom.setRightItem(newTable);
        newFrom.setOuter(from.isOuter());
        newFrom.setRight(from.isRight());
        newFrom.setLeft(from.isLeft());
        newFrom.setNatural(from.isNatural());
        newFrom.setFull(from.isFull());
        newFrom.setInner(from.isInner());
        newFrom.setSimple(from.isSimple());
        newFrom.setCross(from.isCross());
        newFrom.setSemi(from.isSemi());
        // TODO not yet supported
        newFrom.setOnExpression(from.getOnExpression());
        newFrom.setUsingColumns(from.getUsingColumns());
        return newFrom;
    }

    private BinaryExpression buildBinaryExpression(BinaryExpression oldExpression,
            List<OutboundDataOperation.Criterion> newCriterions) {
        BinaryExpression newExpression = null;
        try {
            newExpression = (BinaryExpression) CCJSqlParserUtil.parseCondExpression(oldExpression.toString());
        } catch (JSQLParserException | TokenMgrError e) {
            // Should not occur
            LOGGER.error("Parsing error for {} : ", oldExpression);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Parsing error details:", e);
            }
        }
        Expression leftExpression = newExpression.getLeftExpression();
        Expression rightExpression = newExpression.getRightExpression();
        Column column = (Column) (leftExpression instanceof Column ? leftExpression : rightExpression);
        for (OutboundDataOperation.Criterion newCriterion : newCriterions) {
            String columnName = newCriterion.getDataId().substring(newCriterion.getDataId().lastIndexOf('/') + 1)
                    .toString();
            column.setColumnName(columnName);
            if (newCriterion.getOperator().equals(newExpression.getStringExpression())) {
                String value = newCriterion.getValue().toString();
                if (leftExpression instanceof Column ? newExpression.getRightExpression() instanceof StringValue
                        : newExpression.getLeftExpression() instanceof StringValue) {
                    value = StringUtilities.singleQuote(value);
                }
                Expression newExpr = null;
                try {
                    newExpr = CCJSqlParserUtil.parseCondExpression(value);
                } catch (JSQLParserException | TokenMgrError e) {
                    try {
                        newExpr = CCJSqlParserUtil.parseExpression(value);
                    } catch (JSQLParserException | TokenMgrError e2) {
                        // Should not occur
                        LOGGER.error("Parsing error for {} : ", value);
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("Parsing error details:", e2);
                        }
                    }
                }
                if (leftExpression instanceof Column) {
                    newExpression.setRightExpression(newExpr);
                } else {
                    newExpression.setLeftExpression(newExpr);
                }
            } else if (newExpression instanceof AndExpression
                    && ((leftExpression instanceof Column && rightExpression instanceof Function)
                            || (rightExpression instanceof Column && leftExpression instanceof Function))) {
                Function function = (Function) (leftExpression instanceof Function ? leftExpression : rightExpression);
                if (function.getName().equalsIgnoreCase("st_makeenvelope")
                        && newCriterion.getOperator().equals("area")) {
                    String value = newCriterion.getValue().toString();
                    CCJSqlParser parser = new CCJSqlParser(new StringReader(value));
                    try {
                        ExpressionList parameters = parser.SimpleExpressionList();
                        function.setParameters(parameters);
                    } catch (Exception e) {
                        LOGGER.error("Parsing error for {} : ", value, e);
                    }
                }
            }
        }
        return newExpression;
    }

    private Expression modifyParentExpression(Expression parentExpression, Expression oldExpression,
            Expression newExpression) {
        if (parentExpression instanceof ArrayElement) {
            if (newExpression == null) {
                parentExpression = null;
            } else {
                ArrayElement arrayElement = (ArrayElement) parentExpression;
                if (arrayElement.getLeftExpression() == oldExpression) {
                    arrayElement.setLeftExpression(newExpression);
                } else if (arrayElement.getIndex() == oldExpression) {
                    arrayElement.setIndex(newExpression);
                }
            }
        } else if (parentExpression instanceof Between) {
            if (newExpression == null) {
                parentExpression = null;
            } else {
                Between between = (Between) parentExpression;
                if (between.getLeftExpression() == oldExpression) {
                    between.setLeftExpression(newExpression);
                } else if (between.getBetweenExpressionStart() == oldExpression) {
                    between.setBetweenExpressionStart(newExpression);
                } else if (between.getBetweenExpressionEnd() == oldExpression) {
                    between.setBetweenExpressionEnd(newExpression);
                }
            }
        } else if (parentExpression instanceof Function) {
            Function function = (Function) parentExpression;
            if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
                for (int i = 0; i < function.getParameters().getExpressions().size(); i++) {
                    if (function.getParameters().getExpressions().get(i) == oldExpression) {
                        function.getParameters().getExpressions().set(i,
                                newExpression == null ? new NullValue() : newExpression);
                        break;
                    }
                }
            }
        } else if (parentExpression instanceof InExpression) {
            InExpression inExpression = (InExpression) parentExpression;
            if (inExpression.getLeftExpression() == oldExpression) {
                inExpression.setLeftExpression(newExpression);
            }
        } else if (parentExpression instanceof SignedExpression) {
            if (newExpression == null) {
                parentExpression = null;
            } else {
                SignedExpression signedExpression = (SignedExpression) parentExpression;
                if (signedExpression.getExpression() == oldExpression) {
                    signedExpression.setExpression(newExpression);
                }
            }
        } else if (parentExpression instanceof IsNullExpression) {
            if (newExpression == null) {
                parentExpression = null;
            } else {
                IsNullExpression isNullExpression = (IsNullExpression) parentExpression;
                if (isNullExpression.getLeftExpression() == oldExpression) {
                    isNullExpression.setLeftExpression(newExpression);
                }
            }
        } else if (parentExpression instanceof ExistsExpression) {
            if (newExpression == null) {
                parentExpression = null;
            } else {
                ExistsExpression existsExpression = (ExistsExpression) parentExpression;
                if (existsExpression.getRightExpression() == oldExpression) {
                    existsExpression.setRightExpression(newExpression);
                }
            }
        } else if (parentExpression instanceof Not) {
            if (newExpression == null) {
                parentExpression = null;
            } else {
                Not not = (Not) parentExpression;
                if (not.getExpression() == oldExpression) {
                    not.setExpression(newExpression);
                }
            }
        } else if (parentExpression instanceof Parenthesis) {
            if (newExpression == null) {
                parentExpression = null;
            } else {
                Parenthesis parenthesis = (Parenthesis) parentExpression;
                if (parenthesis.getExpression() == oldExpression) {
                    parenthesis.setExpression(newExpression);
                }
            }
        } else if (parentExpression instanceof BinaryExpression) {
            if (newExpression == null) {
                parentExpression = null;
            } else {
                BinaryExpression binaryExpression = (BinaryExpression) parentExpression;
                if (binaryExpression.getLeftExpression() == oldExpression) {
                    binaryExpression.setLeftExpression(newExpression);
                } else if (binaryExpression.getRightExpression() == oldExpression) {
                    binaryExpression.setRightExpression(newExpression);
                }
            }
        } else if (parentExpression instanceof CaseExpression) {
            CaseExpression caseExpression = (CaseExpression) parentExpression;
            if (caseExpression.getSwitchExpression() == oldExpression) {
                caseExpression.setSwitchExpression(newExpression);
            } else if (caseExpression.getElseExpression() == oldExpression) {
                caseExpression.setElseExpression(newExpression);
            } else if (caseExpression.getWhenClauses() != null) {
                for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
                    if (caseExpression.getWhenClauses().get(i) == oldExpression) {
                        if (newExpression == null) {
                            caseExpression.getWhenClauses().remove(i);
                        } else {
                            caseExpression.getWhenClauses().set(i, newExpression);
                        }
                        break;
                    }
                }
            }
        } else if (parentExpression instanceof WhenClause) {
            if (newExpression == null) {
                parentExpression = null;
            } else {
                WhenClause whenClause = (WhenClause) parentExpression;
                if (whenClause.getWhenExpression() == oldExpression) {
                    whenClause.setWhenExpression(newExpression);
                } else if (whenClause.getThenExpression() == oldExpression) {
                    whenClause.setThenExpression(newExpression);
                }
            }
        } else if (parentExpression instanceof CastExpression) {
            if (newExpression == null) {
                parentExpression = null;
            } else {
                CastExpression castExpression = (CastExpression) parentExpression;
                if (castExpression.getLeftExpression() == oldExpression) {
                    castExpression.setLeftExpression(newExpression);
                }
            }
        } else if (parentExpression instanceof OracleHierarchicalExpression) {
            OracleHierarchicalExpression oracleHierarchicalExpression = (OracleHierarchicalExpression) parentExpression;
            if (oracleHierarchicalExpression.getStartExpression() == oldExpression) {
                oracleHierarchicalExpression.setStartExpression(newExpression);
            } else if (oracleHierarchicalExpression.getConnectExpression() == oldExpression) {
                if (newExpression == null) {
                    parentExpression = null;
                } else {
                    oracleHierarchicalExpression.setConnectExpression(newExpression);
                }
            }
        } else if (parentExpression instanceof WithinGroupExpression) {
            WithinGroupExpression withinGroupExpression = (WithinGroupExpression) parentExpression;
            for (int i = 0; i < withinGroupExpression.getExprList().getExpressions().size(); i++) {
                if (withinGroupExpression.getExprList().getExpressions().get(i) == oldExpression) {
                    if (newExpression == null) {
                        withinGroupExpression.getExprList().getExpressions().remove(i);
                    } else {
                        withinGroupExpression.getExprList().getExpressions().set(i, newExpression);
                    }
                    break;
                }
            }
        } else if (parentExpression instanceof RowConstructor) {
            RowConstructor rowConstructor = (RowConstructor) parentExpression;
            for (int i = 0; i < rowConstructor.getExprList().getExpressions().size(); i++) {
                if (rowConstructor.getExprList().getExpressions().get(i) == oldExpression) {
                    if (newExpression == null) {
                        rowConstructor.getExprList().getExpressions().remove(i);
                    } else {
                        rowConstructor.getExprList().getExpressions().set(i, newExpression);
                    }
                    break;
                }
            }
        }
        return parentExpression;
    }

    private OrderByElement buildOrderByElement(OrderByElement orderByElement, String dataId, List<Table> tables) {
        String[] tokens = dataId.split("/");
        assert tokens.length > 0;
        String newDbName = tokens.length >= 3 ? tokens[tokens.length - 3] : null;
        String newTableName = tokens.length >= 2 ? tokens[tokens.length - 2] : null;
        String newColumnName = tokens[tokens.length - 1];
        Column column = (Column) orderByElement.getExpression();
        Table table = column.getTable();
        Table newTable = null;
        if (table != null && !table.getFullyQualifiedName().isEmpty() && newTableName != null) {
            if (tables.stream().anyMatch(
                    t -> t.getAlias() != null && t.getAlias().getName().equals(table.getFullyQualifiedName()))) {
                newTable = new Table(table.getDatabase(), table.getSchemaName(), table.getName());
            } else {
                Database newDatabase = null;
                if (table.getDatabase() != null && !table.getDatabase().getFullyQualifiedName().isEmpty()
                        && newDbName != null) {
                    newDatabase = new Database(newDbName);
                }
                String[] tokens2 = newTableName.split("\\.");
                if (tokens2.length == 2) {
                    newTableName = tokens2[1];
                }
                String newSchemaName = null;
                if (table.getSchemaName() != null) {
                    newSchemaName = tokens2.length == 2 ? tokens2[0] : "public";
                }
                newTable = new Table(newDatabase, newSchemaName, newTableName);
                newTable.setAlias(table.getAlias());
            }
        }
        OrderByElement newOrderByElement = new OrderByElement();
        newOrderByElement.setAsc(orderByElement.isAsc());
        newOrderByElement.setAscDescPresent(orderByElement.isAscDescPresent());
        newOrderByElement.setNullOrdering(orderByElement.getNullOrdering());
        Column newColumn = new Column(newTable, newColumnName);
        newColumn.setIndex(column.getIndex());
        newOrderByElement.setExpression(newColumn);
        return newOrderByElement;
    }

    private OutboundDataOperation extractDeclareCursorOperation(ChannelHandlerContext ctx, DeclareCursor stmt,
            List<ParameterValue> parameterValues) throws ParseException {
        if (!(stmt.getQuery() instanceof Select)) {
            return null;
        }

        ModuleOperation moduleOperation = extractSelectOperation(ctx, stmt.getQuery(), parameterValues, null, null);
        if (!(moduleOperation instanceof OutboundDataOperation)) {
            throw new IllegalStateException("unexpected");
        }
        OutboundDataOperation outboundDataOperation = (OutboundDataOperation) moduleOperation;
        outboundDataOperation.addAttribute("cursorName", stmt.getName());
        return outboundDataOperation;
    }

    private PgsqlStatement<DeclareCursor> modifyDeclareCursorStatement(ChannelHandlerContext ctx,
            PgsqlStatement<DeclareCursor> statement, OutboundDataOperation outboundDataOperation, boolean newStatement,
            List<ExpectedField> expectedFields) {
        // Prepare statement
        DeclareCursor stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (DeclareCursor) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }
        Select select = stmt.getQuery();
        PgsqlStatement<Select> selectStatement = new PgsqlStatement<>(select, statement.getParameterTypes(),
                statement.getParameterFormats(), statement.getParameterValues(), statement.getResultFormats(),
                statement.getColumns());
        PgsqlStatement<Select> newSelectStatement = modifySelectStatement(ctx, selectStatement, outboundDataOperation,
                false, expectedFields);
        Select newSelect = newSelectStatement.getStatement();
        stmt.setQuery(newSelect);
        return new PgsqlStatement<>(stmt, newSelectStatement.getParameterTypes(),
                newSelectStatement.getParameterFormats(), newSelectStatement.getParameterValues(),
                newSelectStatement.getResultFormats(), newSelectStatement.getColumns());
    }

    private OutboundDataOperation extractCursorFetchOperation(ChannelHandlerContext ctx, CursorFetch stmt)
            throws ParseException {
        OutboundDataOperation outboundDataOperation = new OutboundDataOperation();
        outboundDataOperation.setOperation(null);

        SQLSession session = getSession(ctx);
        CursorContext cursorStatus = session.getCursorContext(stmt.getName());
        if (cursorStatus != null) {
            outboundDataOperation.setDataIds(cursorStatus.getExpectedFields().stream().map(ExpectedField::getAttributes)
                    .flatMap(List::stream).map(Map.Entry::getKey).map(CString::valueOf).collect(Collectors.toList()));
            outboundDataOperation.setPromise(cursorStatus.getPromise());
            outboundDataOperation.setInvolvedCSPs(cursorStatus.getInvolvedBackends());
            outboundDataOperation.setUnprotectingDataEnabled(cursorStatus.isUnprotectingDataEnabled());
        }

        outboundDataOperation.addAttribute("cursorName", stmt.getName());
        return outboundDataOperation;
    }

    private PgsqlStatement<CursorFetch> modifyCursorFetchStatement(ChannelHandlerContext ctx,
            PgsqlStatement<CursorFetch> statement, OutboundDataOperation outboundDataOperation, boolean newStatement) {
        // Prepare statement
        CursorFetch stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (CursorFetch) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }

        return new PgsqlStatement<>(stmt, statement.getParameterTypes(), statement.getParameterFormats(),
                statement.getParameterValues(), statement.getResultFormats(), statement.getColumns());
    }

    private OutboundDataOperation extractCursorCloseOperation(ChannelHandlerContext ctx, CursorClose stmt)
            throws ParseException {
        OutboundDataOperation outboundDataOperation = new OutboundDataOperation();
        outboundDataOperation.setOperation(null);

        SQLSession session = getSession(ctx);
        CursorContext cursorStatus = session.getCursorContext(stmt.getName());
        if (cursorStatus != null) {
            outboundDataOperation.setDataIds(cursorStatus.getExpectedFields().stream().map(ExpectedField::getAttributes)
                    .flatMap(List::stream).map(Map.Entry::getKey).map(CString::valueOf).collect(Collectors.toList()));
            outboundDataOperation.setPromise(cursorStatus.getPromise());
            outboundDataOperation.setInvolvedCSPs(cursorStatus.getInvolvedBackends());
            outboundDataOperation.setUnprotectingDataEnabled(cursorStatus.isUnprotectingDataEnabled());
        }

        outboundDataOperation.addAttribute("cursorName", stmt.getName());
        return outboundDataOperation;
    }

    private PgsqlStatement<CursorClose> modifyCursorCloseStatement(ChannelHandlerContext ctx,
            PgsqlStatement<CursorClose> statement, OutboundDataOperation outboundDataOperation, boolean newStatement) {
        // Prepare statement
        CursorClose stmt = statement.getStatement();
        if (newStatement) {
            // Duplicate statement
            String sql = stmt.toString();
            try {
                stmt = (CursorClose) CCJSqlParserUtil.parse(sql);
            } catch (JSQLParserException | TokenMgrError e) {
                // Should not occur
                LOGGER.error("Parsing error for {} : ", sql);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Parsing error details:", e);
                }
            }
        }

        return new PgsqlStatement<>(stmt, statement.getParameterTypes(), statement.getParameterFormats(),
                statement.getParameterValues(), statement.getResultFormats(), statement.getColumns());
    }

    @Override
    public QueriesTransferMode<BindStep, CommandResults> processBindStep(ChannelHandlerContext ctx, BindStep bindStep)
            throws IOException {
        LOGGER.debug("Bind step: {}", bindStep);
        SQLSession session = getSession(ctx);
        TransferMode transferMode = session.getTransferMode();
        Map<Integer, List<Query>> newQueries = null;
        CommandResults response = null;
        Map<Byte, CString> errorDetails = null;
        ExtendedQueryStatus<ParseStep> parseStepStatus = null;
        ExtendedQueryStatus<BindStep> bindStepStatus = null;
        if (transferMode != TransferMode.ERROR) {
            parseStepStatus = session.getParseStepStatus(bindStep.getPreparedStatement());
            transferMode = TransferMode.FORWARD;
        }
        if (parseStepStatus != null) {
            // Track bind step (for describe, execute and close steps)
            bindStepStatus = session.addBindStep(bindStep, parseStepStatus.getOperation(), parseStepStatus.getType(),
                    parseStepStatus.isToProcess(), parseStepStatus.getInvolvedBackends());
            Operation operation = parseStepStatus.getOperation();
            if (operation != null) {
                CString error = null;
                Mode processingMode;
                switch (operation) {
                case READ:
                    // TODO detect if select is on the whole dataset
                    boolean retrieveWholeDataset = false;
                    processingMode = getProcessingMode(ctx, retrieveWholeDataset, Operation.READ);
                    if (processingMode == null) {
                        // Should not occur
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(String.format("%s read not supported by this CLARUS proxy",
                                retrieveWholeDataset ? "Dataset" : "Record"));
                    } else if (processingMode == Mode.ORCHESTRATION) {
                        // TODO orchestration mode. Meanwhile, same as AS_IT_IS
                        // mode
                        transferMode = TransferMode.FORWARD;
                    } else if (processingMode == Mode.AS_IT_IS || processingMode == Mode.BUFFERING
                            || processingMode == Mode.STREAMING) {
                        transferMode = TransferMode.FORWARD;
                    }
                    break;
                case CREATE:
                    boolean inDatasetCreation = session.isInDatasetCreation();
                    processingMode = getProcessingMode(ctx, inDatasetCreation, Operation.CREATE);
                    if (processingMode == null) {
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(String.format("%s creation not supported by this CLARUS proxy",
                                inDatasetCreation ? "Dataset" : "Record"));
                    } else if (processingMode == Mode.BUFFERING) {
                        if (inDatasetCreation) {
                            transferMode = TransferMode.FORGET;
                        } else {
                            // Should not occur
                            transferMode = TransferMode.ERROR;
                            error = CString.valueOf(
                                    "Buffering processing mode not supported for record creation by this CLARUS proxy");
                        }
                    } else if (processingMode == Mode.ORCHESTRATION) {
                        // Should not occur
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(
                                "Orchestration processing mode not supported for dataset or record creation by this CLARUS proxy");
                    } else if (processingMode == Mode.AS_IT_IS || processingMode == Mode.STREAMING) {
                        transferMode = TransferMode.FORWARD;
                    }
                    break;
                case UPDATE:
                    // TODO detect if update is on the whole dataset
                    boolean inDatasetModification = false;
                    processingMode = getProcessingMode(ctx, inDatasetModification, Operation.UPDATE);
                    if (processingMode == null) {
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(String.format("%s modification not supported by this CLARUS proxy",
                                inDatasetModification ? "Dataset" : "Record"));
                    } else if (processingMode == Mode.BUFFERING) {
                        if (inDatasetModification) {
                            transferMode = TransferMode.FORGET;
                        } else {
                            // Should not occur
                            transferMode = TransferMode.ERROR;
                            error = CString.valueOf(
                                    "Buffering processing mode not supported for record modification by this CLARUS proxy");
                        }
                    } else if (processingMode == Mode.ORCHESTRATION) {
                        // Should not occur
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(
                                "Orchestration processing mode not supported for dataset or record modification by this CLARUS proxy");
                    } else if (processingMode == Mode.AS_IT_IS || processingMode == Mode.STREAMING) {
                        transferMode = TransferMode.FORWARD;
                    }
                    break;
                case DELETE:
                    boolean deleteWholeDataset = parseStepStatus.getType() == SQLCommandType.DROP_TABLE;
                    processingMode = getProcessingMode(ctx, deleteWholeDataset, Operation.DELETE);
                    if (processingMode == null) {
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(String.format("%s delete not supported by this CLARUS proxy",
                                deleteWholeDataset ? "Dataset" : "Record"));
                    } else if (processingMode == Mode.BUFFERING) {
                        if (deleteWholeDataset) {
                            transferMode = TransferMode.FORGET;
                        } else {
                            // Should not occur
                            transferMode = TransferMode.ERROR;
                            error = CString.valueOf(
                                    "Buffering processing mode not supported for record delete by this CLARUS proxy");
                        }
                    } else if (processingMode == Mode.ORCHESTRATION) {
                        // Should not occur
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf(
                                "Orchestration processing mode not supported for dataset or record delete by this CLARUS proxy");
                    } else if (processingMode == Mode.AS_IT_IS || processingMode == Mode.STREAMING) {
                        transferMode = TransferMode.FORWARD;
                    }
                    break;
                default:
                    break;
                }
                if (error != null) {
                    errorDetails = new LinkedHashMap<>();
                    errorDetails.put((byte) 'S', CString.valueOf("FATAL"));
                    errorDetails.put((byte) 'M', error);
                }
            }
        }
        session.setTransferMode(transferMode);
        if (transferMode == TransferMode.FORWARD) {
            if (parseStepStatus != null) {
                // Set involved backends
                session.setCommandInvolvedBackends(Collections.singletonList(getPreferredBackend(ctx)), false);
                DescribeStepStatus describeStepStatus = session.getDescribeStepStatus((byte) 'S',
                        parseStepStatus.getQuery().getName());
                Result<List<Query>, CommandResults, CString> result = buildNewQueries(ctx, parseStepStatus,
                        describeStepStatus, bindStepStatus);
                if (result.isQuery()) {
                    // Retrieve modified (or unmodified) queries to forward
                    newQueries = result.queriesAsMap();
                } else if (result.isResponse()) {
                    // Forget the query and reply immediately
                    transferMode = TransferMode.FORGET;
                    response = result.response();
                    newQueries = null;
                } else if (result.isError()) {
                    transferMode = TransferMode.ERROR;
                    errorDetails = new LinkedHashMap<>();
                    errorDetails.put((byte) 'S', CString.valueOf("FATAL"));
                    errorDetails.put((byte) 'M', result.error());
                    if (session.getTransactionStatus() == (byte) 'T') {
                        session.setTransactionStatus((byte) 'E');
                        session.setTransactionErrorDetails(errorDetails);
                    }
                    session.resetCurrentCommand();
                    newQueries = null;
                }
            } else {
                newQueries = Collections.singletonMap(getPreferredBackend(ctx), Collections.singletonList(bindStep));
            }
        } else if (transferMode == TransferMode.FORGET) {
            // Buffer extended queries
            if (parseStepStatus != null) {
                errorDetails = bufferQuery(ctx, parseStepStatus.getQuery());
            }
            if (errorDetails == null) {
                errorDetails = bufferQuery(ctx, bindStep);
            }
            if (errorDetails != null) {
                transferMode = TransferMode.ERROR;
                session.resetCurrentCommand();
            } else {
                response = new CommandResults();
                response.setBindCompleteRequired(true);
            }
            newQueries = null;
        } else if (transferMode == TransferMode.ERROR) {
            if (errorDetails != null) {
                if (session.getTransactionStatus() == (byte) 'T') {
                    session.setTransactionStatus((byte) 'E');
                    session.setTransactionErrorDetails(errorDetails);
                }
                session.resetCurrentCommand();
            } else {
                // just forget query
                transferMode = TransferMode.FORGET;
                response = new CommandResults();
            }
            newQueries = null;
        }
        QueriesTransferMode<BindStep, CommandResults> mode = new QueriesTransferMode<>(transferMode, newQueries,
                response, errorDetails);
        LOGGER.debug("Bind step processed: new queries={}, transfer mode={}", mode.getNewQueries(),
                mode.getTransferMode());
        return mode;
    }

    @Override
    public QueriesTransferMode<DescribeStep, CommandResults> processDescribeStep(ChannelHandlerContext ctx,
            DescribeStep describeStep) throws IOException {
        LOGGER.debug("Describe step: {}", describeStep);
        SQLSession session = getSession(ctx);
        TransferMode transferMode = session.getTransferMode();
        Map<Integer, List<Query>> newQueries = null;
        boolean buildResponse = false;
        CommandResults response = null;
        Map<Byte, CString> errorDetails = null;
        if (transferMode == TransferMode.FORWARD) {
            DescribeStepStatus describeStepStatus = session.addDescribeStep(describeStep);
            if (describeStep.getCode() == 'S') {
                // Postpone processing of extended query
                transferMode = TransferMode.FORGET;
                buildResponse = true;
                newQueries = null;
            } else if (describeStep.getCode() == 'P') {
                ParseStep parseStep = null;
                ExtendedQueryStatus<BindStep> bindStepStatus = session.getBindStepStatus(describeStep.getName());
                if (bindStepStatus != null) {
                    BindStep bindStep = bindStepStatus.getQuery();
                    ExtendedQueryStatus<ParseStep> parseStepStatus = session
                            .getParseStepStatus(bindStep.getPreparedStatement());
                    parseStep = parseStepStatus.getQuery();
                }
                if (parseStep != null && bindStepStatus != null) {
                    if (parseStep.isMetadata()) {
                        transferMode = TransferMode.FORGET;
                        buildResponse = true;
                        newQueries = null;
                    } else {
                        // Set involved backends
                        session.setCommandInvolvedBackends(bindStepStatus.getInvolvedBackends());
                        if (session.getCommandInvolvedBackends().isEmpty()) {
                            transferMode = TransferMode.FORGET;
                            buildResponse = true;
                        } else {
                            // Forward query to all involved backends
                            List<Integer> involvedBackends = session.getCommandInvolvedBackends();
                            if (involvedBackends.size() > 1) {
                                newQueries = involvedBackends.stream()
                                        .map(backend -> new SimpleEntry<>(backend,
                                                Collections.<Query>singletonList(new DescribeStep(
                                                        describeStep.getCode(), describeStep.getName()))))
                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                            } else {
                                newQueries = Collections.singletonMap(involvedBackends.get(0),
                                        Collections.singletonList(describeStep));
                            }
                            // Retain queries
                            newQueries.values().stream().filter(l -> l != null).flatMap(List::stream)
                                    .filter(q -> q != null).forEach(Query::retain);
                            // Set as current describe step
                            session.setCurrentDescribeStepStatus(describeStepStatus);
                        }
                    }
                } else {
                    newQueries = Collections.singletonMap(getPreferredBackend(ctx),
                            Collections.singletonList(describeStep));
                }
            }
        } else if (transferMode == TransferMode.FORGET) {
            // Buffer extended query
            errorDetails = bufferQuery(ctx, describeStep);
            if (errorDetails != null) {
                transferMode = TransferMode.ERROR;
                session.resetCurrentCommand();
            }
            buildResponse = true;
            newQueries = null;
        } else if (transferMode == TransferMode.ERROR) {
            // just forget query
            transferMode = TransferMode.FORGET;
            buildResponse = false;
            response = new CommandResults();
            newQueries = null;
        }
        if (transferMode == TransferMode.FORGET && buildResponse) {
            // Build response
            response = new CommandResults();
            ParseStep parseStep = null;
            if (describeStep.getCode() == 'S') {
                ExtendedQueryStatus<ParseStep> parseStepStatus = session.getParseStepStatus(describeStep.getName());
                parseStep = parseStepStatus.getQuery();
                // TODO parse SQL statement to extract exact list of parameter
                if (parseStep != null && parseStep.getParameterTypes() != null) {
                    List<Long> parameterTypes = new ArrayList<Long>(parseStep.getParameterTypes());
                    response.setParameterDescription(parameterTypes);
                } else {
                    response.setParameterDescription(Collections.emptyList());
                }
            } else if (describeStep.getCode() == 'P') {
                ExtendedQueryStatus<BindStep> bindStepStatus = session.getBindStepStatus(describeStep.getName());
                if (bindStepStatus != null) {
                    BindStep bindStep = bindStepStatus.getQuery();
                    ExtendedQueryStatus<ParseStep> parseStepStatus = session
                            .getParseStepStatus(bindStep.getPreparedStatement());
                    parseStep = parseStepStatus.getQuery();
                }
            }
            // Build row description
            List<PgsqlRowDescriptionMessage.Field> rowDescription;
            if (parseStep != null && parseStep.getColumns() != null) {
                if (parseStep.isMetadata()) {
                    rowDescription = Stream.of("column_name", "protected_column_name").map(CString::valueOf)
                            .map(PgsqlRowDescriptionMessage.Field::new).peek(f -> f.setTypeOID(705))
                            .peek(f -> f.setTypeSize((short) -2)).peek(f -> f.setTypeModifier(-1))
                            .collect(Collectors.toList());
                } else {
                    rowDescription = parseStep.getColumns().stream().map(PgsqlRowDescriptionMessage.Field::new)
                            .collect(Collectors.toList());
                }
            } else {
                // TODO parse SQL statement to extract selected columns (to row
                // description)
                // empty list means no data
                rowDescription = Collections.emptyList();
            }
            response.setRowDescription(rowDescription);
        }
        QueriesTransferMode<DescribeStep, CommandResults> mode = new QueriesTransferMode<>(transferMode, newQueries,
                response, errorDetails);
        LOGGER.debug("Describe step processed: new queries={}, transfer mode={}", mode.getNewQueries(),
                mode.getTransferMode());
        return mode;
    }

    @Override
    public QueriesTransferMode<ExecuteStep, CommandResults> processExecuteStep(ChannelHandlerContext ctx,
            ExecuteStep executeStep) throws IOException {
        LOGGER.debug("Execute step: {}", executeStep);
        SQLSession session = getSession(ctx);
        TransferMode transferMode = session.getTransferMode();
        Map<Integer, List<Query>> newQueries = null;
        CommandResults response = null;
        Map<Byte, CString> errorDetails = null;
        if (transferMode == TransferMode.FORWARD) {
            ParseStep parseStep = null;
            ExtendedQueryStatus<BindStep> bindStepStatus = session.getBindStepStatus(executeStep.getPortal());
            if (bindStepStatus != null) {
                BindStep bindStep = bindStepStatus.getQuery();
                ExtendedQueryStatus<ParseStep> parseStepStatus = session
                        .getParseStepStatus(bindStep.getPreparedStatement());
                parseStep = parseStepStatus.getQuery();
            }
            if (parseStep != null && bindStepStatus != null) {
                if (parseStep.isMetadata()) {
                    transferMode = TransferMode.FORGET;
                    // Process metadata operation
                    MetadataOperation metadataOperation = new MetadataOperation();
                    metadataOperation.setDataIds(parseStep.getColumns());
                    metadataOperation = newMetaDataOperation(ctx, metadataOperation);
                    // Forget SQL statement, reply directly to the frontend
                    response = new CommandResults();
                    if (metadataOperation.isModified()) {
                        List<Map.Entry<CString, List<CString>>> metadata = metadataOperation.getMetadata();
                        // Verify all data ids refer to the same dataset (prefix
                        // is
                        // the same for all data ids)
                        Set<CString> prefixes = metadata.stream().map(Map.Entry::getKey)
                                .map(id -> id.substring(0, id.lastIndexOf('/'))).collect(Collectors.toSet());
                        boolean multipleDatasets = prefixes.size() > 1
                                || prefixes.stream().findFirst().get().equals("*");
                        if (!multipleDatasets) {
                            // Prefix is the same for all data ids -> remove
                            // prefix
                            metadata = metadata.stream()
                                    .map(e -> new SimpleEntry<>(e.getKey().substring(e.getKey().lastIndexOf('/') + 1),
                                            e.getValue()))
                                    .collect(Collectors.toList());
                        }
                        // Build rows
                        List<List<ByteBuf>> rows = buildRows(metadata, bindStepStatus.getInvolvedBackends(),
                                executeStep.getMaxRows());
                        response.setRows(rows);
                        // Build complete tag
                        response.setCompleteTag(CString.valueOf("SELECT " + rows.size()));
                    } else {
                        // Build complete tag
                        response.setCompleteTag(CString.valueOf("SELECT 0"));
                    }
                    newQueries = null;
                } else {
                    // Set involved backends
                    session.setCommandInvolvedBackends(bindStepStatus.getInvolvedBackends());
                    if (session.getCommandInvolvedBackends().isEmpty()) {
                        // Forget SQL statement, reply directly to the frontend
                        transferMode = TransferMode.FORGET;
                        response = new CommandResults();
                        response.setCompleteTag(CString.valueOf("SELECT 0"));
                    } else {
                        // Forward query to all involved backends
                        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
                        if (involvedBackends.size() > 1) {
                            newQueries = involvedBackends.stream()
                                    .map(backend -> new SimpleEntry<>(backend,
                                            Collections.<Query>singletonList(new ExecuteStep(executeStep.getPortal(),
                                                    executeStep.getMaxRows()))))
                                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                        } else {
                            newQueries = Collections.singletonMap(involvedBackends.get(0),
                                    Collections.singletonList(executeStep));
                        }
                        // Retain queries
                        newQueries.values().stream().filter(l -> l != null).flatMap(List::stream).filter(q -> q != null)
                                .forEach(Query::retain);
                        // Ensure description step is present
                        if (session.getCurrentDescribeStepStatus() == null) {
                            DescribeStepStatus describeStepStatus = session.getDescribeStepStatus((byte) 'P',
                                    bindStepStatus.getQuery().getName());
                            if (describeStepStatus == null) {
                                describeStepStatus = session.getDescribeStepStatus((byte) 'S', parseStep.getName());
                            }
                            // Set as current describe step
                            session.setCurrentDescribeStepStatus(describeStepStatus);
                        }
                    }
                }
            } else {
                newQueries = Collections.singletonMap(getPreferredBackend(ctx), Collections.singletonList(executeStep));
            }
        } else if (transferMode == TransferMode.FORGET) {
            ExtendedQueryStatus<BindStep> bindStepStatus = session.getBindStepStatus(executeStep.getPortal());
            if (bindStepStatus != null) {
                response = new CommandResults();
                Operation operation = bindStepStatus.getOperation();
                if (operation == null) {
                    SQLCommandType type = bindStepStatus.getType();
                    if (type != null) {
                        switch (type) {
                        case SET:
                            response.setCompleteTag(CString.valueOf("SET"));
                            break;
                        case FETCH_CURSOR:
                            response.setCompleteTag(CString.valueOf("FETCH 0"));
                            break;
                        case CLOSE_CURSOR:
                            response.setCompleteTag(CString.valueOf("CLOSE CURSOR"));
                            break;
                        case START_TRANSACTION:
                            response.setCompleteTag(CString.valueOf("BEGIN"));
                            break;
                        case COMMIT:
                            response.setCompleteTag(CString.valueOf("COMMIT"));
                            break;
                        default:
                            break;
                        }
                    }
                } else {
                    switch (operation) {
                    case READ: {
                        SQLCommandType type = bindStepStatus.getType();
                        if (type != null) {
                            switch (type) {
                            case SELECT:
                                response.setCompleteTag(CString.valueOf("SELECT 0"));
                                break;
                            case DECLARE_CURSOR:
                                response.setCompleteTag(CString.valueOf("DECLARE CURSOR"));
                                break;
                            default:
                                break;
                            }
                        }
                        break;
                    }
                    case CREATE: {
                        SQLCommandType type = bindStepStatus.getType();
                        if (type != null) {
                            switch (type) {
                            case CREATE_TABLE:
                                response.setCompleteTag(CString.valueOf("CREATE TABLE"));
                                break;
                            case ALTER_TABLE:
                                response.setCompleteTag(CString.valueOf("ALTER TABLE"));
                                break;
                            case ADD_GEOMETRY_COLUMN:
                                response.setCompleteTag(CString.valueOf("SELECT 1"));
                                break;
                            case INSERT:
                                response.setCompleteTag(CString.valueOf("INSERT 0 1"));
                                break;
                            default:
                                break;
                            }
                        }
                        break;
                    }
                    case UPDATE: {
                        SQLCommandType type = bindStepStatus.getType();
                        if (type != null) {
                            switch (type) {
                            case ALTER_TABLE:
                                response.setCompleteTag(CString.valueOf("ALTER TABLE"));
                                break;
                            case ADD_GEOMETRY_COLUMN:
                                response.setCompleteTag(CString.valueOf("SELECT 1"));
                                break;
                            case UPDATE:
                                response.setCompleteTag(CString.valueOf("UPDATE 0 1"));
                                break;
                            default:
                                break;
                            }
                        }
                        break;
                    }
                    case DELETE: {
                        SQLCommandType type = bindStepStatus.getType();
                        if (type != null) {
                            switch (type) {
                            case DROP_TABLE:
                                response.setCompleteTag(CString.valueOf("DROP TABLE"));
                                break;
                            case DELETE:
                                response.setCompleteTag(CString.valueOf("DELETE 0 1"));
                                break;
                            default:
                                break;
                            }
                        }
                        break;
                    }
                    default:
                        break;
                    }
                }
                // Buffer extended query
                errorDetails = bufferQuery(ctx, executeStep);
                if (errorDetails != null) {
                    transferMode = TransferMode.ERROR;
                }
                newQueries = null;
            } else {
                transferMode = TransferMode.FORWARD;
                newQueries = Collections.singletonMap(getPreferredBackend(ctx), Collections.singletonList(executeStep));
            }
        } else if (transferMode == TransferMode.ERROR) {
            // just forget query
            transferMode = TransferMode.FORGET;
            response = new CommandResults();
            newQueries = null;
        }
        QueriesTransferMode<ExecuteStep, CommandResults> mode = new QueriesTransferMode<>(transferMode, newQueries,
                response, errorDetails);
        LOGGER.debug("Execute step processed: new queries={}, transfer mode={}", mode.getNewQueries(),
                mode.getTransferMode());
        return mode;
    }

    private List<List<ByteBuf>> buildRows(List<Map.Entry<CString, List<CString>>> metadata,
            List<Integer> involvedBackends, int maxSize) {
        // Build rows (replacing / by .)
        List<List<ByteBuf>> rows = metadata.stream().flatMap(e -> {
            CString key = e.getKey().replace('/', '.');
            ByteBuf clearColumn = key.getByteBuf(key.length());
            List<CString> values = e.getValue();
            List<ByteBuf> protectedColumns = IntStream.range(0, values.size()).mapToObj(csp -> {
                CString value = values.get(csp);
                ByteBuf protectedColumn = null;
                if (value != null) {
                    value = CString.valueOf("csp" + (csp + 1)).append('.').append(value.replace('/', '.'));
                    protectedColumn = value.getByteBuf(value.length());
                }
                return protectedColumn;
            }).filter(pc -> pc != null).collect(Collectors.toList());
            if (protectedColumns.isEmpty()) {
                protectedColumns.add(null);
            }
            if (protectedColumns.size() > 1) {
                clearColumn.retain(protectedColumns.size() - 1);
            }
            return protectedColumns.stream().map(pc -> Stream.of(clearColumn, pc).collect(Collectors.toList()));
        }).limit(maxSize < 0 ? Integer.MAX_VALUE : maxSize).collect(Collectors.toList());
        return rows;
    }

    @Override
    public QueriesTransferMode<CloseStep, CommandResults> processCloseStep(ChannelHandlerContext ctx,
            CloseStep closeStep) throws IOException {
        LOGGER.debug("Close step: {}", closeStep);
        SQLSession session = getSession(ctx);
        TransferMode transferMode = session.getTransferMode();
        Map<Integer, List<Query>> newQueries = null;
        CommandResults response = null;
        Map<Byte, CString> errorDetails = null;
        if (transferMode == TransferMode.FORWARD) {
            ExtendedQueryStatus<ParseStep> parseStepStatus = null;
            ExtendedQueryStatus<BindStep> bindStepStatus = null;
            if (closeStep.getCode() == 'S') {
                parseStepStatus = session.getParseStepStatus(closeStep.getName());
                if (parseStepStatus != null) {
                    ParseStep parseStep = parseStepStatus.getQuery();
                    if (parseStep.isMetadata()) {
                        transferMode = TransferMode.FORGET;
                        response = new CommandResults();
                        response.setCloseCompleteRequired(true);
                        newQueries = null;
                    }
                    // Set involved backends
                    session.setCommandInvolvedBackends(parseStepStatus.getInvolvedBackends());
                    session.removeParseStep(closeStep.getName());
                }
            } else {
                bindStepStatus = session.getBindStepStatus(closeStep.getName());
                if (bindStepStatus != null) {
                    // Set involved backends
                    session.setCommandInvolvedBackends(bindStepStatus.getInvolvedBackends());
                    session.removeBindStep(closeStep.getName());
                }
            }
            if (transferMode == TransferMode.FORWARD) {
                if (parseStepStatus != null || bindStepStatus != null) {
                    if (session.getCommandInvolvedBackends().isEmpty()) {
                        // Forget SQL statement, reply directly to the frontend
                        transferMode = TransferMode.FORGET;
                        response = new CommandResults();
                        response.setCloseCompleteRequired(true);
                    } else {
                        // Forward query to all involved backends
                        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
                        if (involvedBackends.size() > 1) {
                            newQueries = involvedBackends.stream()
                                    .map(backend -> new SimpleEntry<>(backend,
                                            Collections.<Query>singletonList(
                                                    new CloseStep(closeStep.getCode(), closeStep.getName()))))
                                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                        } else {
                            newQueries = Collections.singletonMap(involvedBackends.get(0),
                                    Collections.singletonList(closeStep));
                        }
                        // Retain queries
                        newQueries.values().stream().filter(l -> l != null).flatMap(List::stream).filter(q -> q != null)
                                .forEach(Query::retain);
                    }
                } else {
                    newQueries = Collections.singletonMap(getPreferredBackend(ctx),
                            Collections.singletonList(closeStep));
                }
            }
        } else if (transferMode == TransferMode.FORGET) {
            // Buffer extended query
            errorDetails = bufferQuery(ctx, closeStep);
            if (errorDetails != null) {
                transferMode = TransferMode.ERROR;
            } else {
                response = new CommandResults();
                response.setCloseCompleteRequired(true);
            }
            newQueries = null;
        } else if (transferMode == TransferMode.ERROR) {
            // just forget query
            transferMode = TransferMode.FORGET;
            response = new CommandResults();
            newQueries = null;
        }
        QueriesTransferMode<CloseStep, CommandResults> mode = new QueriesTransferMode<>(transferMode, newQueries,
                response, errorDetails);
        LOGGER.debug("Close step processed: new queries={}, transfer mode={}", mode.getNewQueries(),
                mode.getTransferMode());
        return mode;
    }

    @Override
    public QueriesTransferMode<SynchronizeStep, Byte> processSynchronizeStep(ChannelHandlerContext ctx,
            SynchronizeStep synchronizeStep) throws IOException {
        LOGGER.debug("Synchronize step: {}", synchronizeStep);
        SQLSession session = getSession(ctx);
        TransferMode transferMode = session.getTransferMode();
        Map<Integer, List<Query>> newQueries = null;
        Byte response = null;
        Map<Byte, CString> errorDetails = null;
        if (transferMode == TransferMode.FORWARD) {
            if (session.getCommandInvolvedBackends() != null) {
                if (session.getCommandInvolvedBackends().isEmpty()) {
                    transferMode = TransferMode.FORGET;
                    response = session.getTransactionStatus();
                    // reset current command in session
                    session.resetCurrentCommand();
                } else {
                    // Forward query to all involved backends
                    List<Integer> involvedBackends = session.getCommandInvolvedBackends();
                    if (involvedBackends.size() > 1) {
                        newQueries = involvedBackends.stream()
                                .map(backend -> new SimpleEntry<>(backend,
                                        Collections.<Query>singletonList(new SynchronizeStep())))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    } else {
                        newQueries = Collections.singletonMap(involvedBackends.get(0),
                                Collections.singletonList(synchronizeStep));
                    }
                    // Retain queries
                    newQueries.values().stream().filter(l -> l != null).flatMap(List::stream).filter(q -> q != null)
                            .forEach(Query::retain);
                }
            } else {
                newQueries = Collections.singletonMap(getPreferredBackend(ctx),
                        Collections.singletonList(synchronizeStep));
            }
        } else if (transferMode == TransferMode.FORGET) {
            response = session.getTransactionStatus();
            // Buffer extended query
            errorDetails = bufferQuery(ctx, synchronizeStep);
            if (errorDetails != null) {
                transferMode = TransferMode.ERROR;
            }
            newQueries = null;
        } else if (transferMode == TransferMode.ERROR) {
            // Reset transfer mode for next queries
            session.setTransferMode(TransferMode.FORWARD);
            // Reply with ready for query
            transferMode = TransferMode.FORGET;
            response = session.getTransactionStatus();
            newQueries = null;
        }
        QueriesTransferMode<SynchronizeStep, Byte> mode = new QueriesTransferMode<>(transferMode, newQueries, response,
                errorDetails);
        LOGGER.debug("Synchronize step processed: new queries={}, transfer mode={}", mode.getNewQueries(),
                mode.getTransferMode());
        return mode;
    }

    @Override
    public QueriesTransferMode<FlushStep, Void> processFlushStep(ChannelHandlerContext ctx, FlushStep flushStep)
            throws IOException {
        LOGGER.debug("Flush step: {}", flushStep);
        SQLSession session = getSession(ctx);
        TransferMode transferMode = session.getTransferMode();
        Map<Integer, List<Query>> newQueries = null;
        Void response = null;
        Map<Byte, CString> errorDetails = null;
        if (transferMode == TransferMode.FORWARD) {
            if (session.getCommandInvolvedBackends() != null) {
                if (session.getCommandInvolvedBackends().isEmpty()) {
                    // Forget SQL statement, reply directly to the frontend
                    transferMode = TransferMode.FORGET;
                } else {
                    // Forward query to all involved backends
                    List<Integer> involvedBackends = session.getCommandInvolvedBackends();
                    if (involvedBackends.size() > 1) {
                        newQueries = involvedBackends.stream()
                                .map(backend -> new SimpleEntry<>(backend,
                                        Collections.<Query>singletonList(new FlushStep())))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    } else {
                        newQueries = Collections.singletonMap(involvedBackends.get(0),
                                Collections.singletonList(flushStep));
                    }
                    // Retain queries
                    newQueries.values().stream().filter(l -> l != null).flatMap(List::stream).filter(q -> q != null)
                            .forEach(Query::retain);
                }
            } else {
                newQueries = Collections.singletonMap(getPreferredBackend(ctx), Collections.singletonList(flushStep));
            }
        } else if (transferMode == TransferMode.FORGET) {
            // Buffer extended query
            errorDetails = bufferQuery(ctx, flushStep);
            if (errorDetails != null) {
                transferMode = TransferMode.ERROR;
            }
            newQueries = null;
        } else if (transferMode == TransferMode.ERROR) {
            // just forget query
            transferMode = TransferMode.FORGET;
            response = null;
            newQueries = null;
        }
        QueriesTransferMode<FlushStep, Void> mode = new QueriesTransferMode<>(transferMode, newQueries, response,
                errorDetails);
        LOGGER.debug("Flush step processed: new queries={}, transfer mode={}", mode.getNewQueries(),
                mode.getTransferMode());
        return mode;
    }

    private Result<List<Query>, CommandResults, CString> buildNewQueries(ChannelHandlerContext ctx,
            ExtendedQueryStatus<ParseStep> parseStepStatus, DescribeStepStatus describeStepStatus,
            ExtendedQueryStatus<BindStep> bindStepStatus) throws IOException {
        SQLSession session = getSession(ctx);
        // Set involved backends
        session.setCommandInvolvedBackends(bindStepStatus.getInvolvedBackends());
        List<List<ExtendedQuery>> newExtendedQueries = null;
        if (parseStepStatus.isToProcess()) {
            Result<List<ExtendedQuery>, CommandResults, CString> result = processExtendedQuery(ctx, parseStepStatus,
                    describeStepStatus, bindStepStatus);
            if (result.isQuery()) {
                newExtendedQueries = result.queries();
            } else if (result.isResponse()) {
                return Result.response(result.response());
            } else if (result.isError()) {
                return Result.error(result.error());
            }
        } else {
            List<ExtendedQuery> queries = new ArrayList<>();
            queries.add(parseStepStatus.getQuery());
            if (describeStepStatus != null) {
                queries.add(describeStepStatus.getQuery());
                // Set as current describe step
                session.setCurrentDescribeStepStatus(describeStepStatus);
            }
            queries.add(bindStepStatus.getQuery());
            newExtendedQueries = Collections.singletonList(queries);
            // ignore response
            session.addLastQueryResponseToIgnore(QueryResponseType.PARSE_COMPLETE);
            if (describeStepStatus != null) {
                // ignore responses
                session.addLastQueryResponseToIgnore(QueryResponseType.PARAMETER_DESCRIPTION);
                session.addLastQueryResponseToIgnore(QueryResponseType.ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA);
            }
        }

        // Retain queries
        newExtendedQueries.stream().filter(l -> l != null).flatMap(List::stream).filter(q -> q != null)
                .forEach(Query::retain);

        List<List<Query>> bufferedQueries = processBufferedQueries(ctx);
        List<List<Query>> newQueries = null;
        if (bufferedQueries.isEmpty() || bufferedQueries.stream().allMatch(List::isEmpty)) {
            newQueries = newExtendedQueries.stream()
                    .map(l -> l != null ? l.stream().collect(Collectors.<Query>toList()) : null)
                    .collect(Collectors.toList());
        } else {
            int nbDirected = bufferedQueries.size() > newExtendedQueries.size() ? bufferedQueries.size()
                    : newExtendedQueries.size();
            newQueries = new ArrayList<>(nbDirected);
            for (int i = 0; i < nbDirected; i++) {
                List<? extends Query> directedBufferedQueries = i < bufferedQueries.size() ? bufferedQueries.get(i)
                        : null;
                if (directedBufferedQueries == null) {
                    directedBufferedQueries = Collections.emptyList();
                }
                List<? extends Query> directedExtendedQueries = i < newExtendedQueries.size()
                        ? newExtendedQueries.get(i) : null;
                if (directedExtendedQueries == null) {
                    directedExtendedQueries = Collections.emptyList();
                }
                List<Query> directedQueries = Stream
                        .concat(directedBufferedQueries.stream(), directedExtendedQueries.stream())
                        .collect(Collectors.<Query>toList());
                newQueries.add(directedQueries);
            }
        }
        return Result.queries(newQueries);
    }

    private Result<List<ExtendedQuery>, CommandResults, CString> processExtendedQuery(ChannelHandlerContext ctx,
            ExtendedQueryStatus<ParseStep> parseStepStatus, DescribeStepStatus describeStepStatus,
            ExtendedQueryStatus<BindStep> bindStepStatus) {
        ParseStep parseStep = parseStepStatus.getQuery();
        BindStep bindStep = bindStepStatus.getQuery();
        // Parse SQL statement
        if (parseStep.getSQL().isBuffered()) {
            parseStep.getSQL().getByteBuf().readerIndex(0);
        }
        Statement stmt = parseSQL(ctx, parseStep.getSQL());
        SQLSession session = getSession(ctx);
        Result<List<ExtendedQuery>, CommandResults, CString> result = null;
        if (stmt != null) {
            // Build bind parameter (type+format+value)
            // create a ParameterValue for each parameter value
            List<ParameterValue> parameterValues = bindStep.getParameterValues().stream().map(ParameterValue::new)
                    .collect(Collectors.toList()); // build a list
            if (!parseStep.getParameterTypes().isEmpty()) {
                final List<Long> parameterTypes = parseStep.getParameterTypes();
                for (int idx = 0; idx < parameterTypes.size(); idx++) {
                    // set parameter type
                    parameterValues.get(idx).setType(parameterTypes.get(idx));
                }
            }
            if (!bindStep.getParameterFormats().isEmpty()) {
                final List<Short> formats = bindStep.getParameterFormats();
                for (int i = 0; i < parameterValues.size(); i++) {
                    ParameterValue parameterValue = parameterValues.get(i);
                    Short format = formats.get(formats.size() == 1 ? 0 : i);
                    // set parameter value format
                    parameterValue.setFormat(format);
                }
            }
            // Extract module operation
            ModuleOperation moduleOperation = null;
            try {
                if (stmt instanceof SetStatement) {
                    moduleOperation = extractSetOperation(ctx, (SetStatement) stmt);
                } else if (stmt instanceof StartTransaction) {
                    moduleOperation = extractStartTransactionOperation(ctx, (StartTransaction) stmt);
                } else if (stmt instanceof Commit) {
                    moduleOperation = extractCommitOperation(ctx, (Commit) stmt);
                } else if (stmt instanceof CreateTable) {
                    moduleOperation = extractCreateTableOperation(ctx, (CreateTable) stmt);
                } else if (stmt instanceof Alter) {
                    moduleOperation = extractAlterTableOperation(ctx, (Alter) stmt, null,
                            bindStepStatus.getOperation());
                } else if (stmt instanceof CreateIndex) {
                    moduleOperation = extractCreateIndexOperation(ctx, (CreateIndex) stmt, null,
                            bindStepStatus.getOperation());
                } else if (stmt instanceof Drop) {
                    moduleOperation = extractDropTableOperation(ctx, (Drop) stmt);
                } else if (stmt instanceof Insert) {
                    moduleOperation = extractInsertOperation(ctx, (Insert) stmt, parameterValues, null);
                } else if (stmt instanceof Update) {
                    // TODO Update
                } else if (stmt instanceof Delete) {
                    // TODO Delete
                } else if (stmt instanceof Select) {
                    moduleOperation = extractSelectOperation(ctx, (Select) stmt, parameterValues, null,
                            bindStepStatus.getOperation());
                } else if (stmt instanceof DeclareCursor) {
                    moduleOperation = extractDeclareCursorOperation(ctx, (DeclareCursor) stmt, parameterValues);
                } else if (stmt instanceof CursorFetch) {
                    moduleOperation = extractCursorFetchOperation(ctx, (CursorFetch) stmt);
                } else if (stmt instanceof CursorClose) {
                    moduleOperation = extractCursorCloseOperation(ctx, (CursorClose) stmt);
                }
            } catch (ParseException e) {
                return Result.error(CString.valueOf(e.getMessage()));
            }
            if (moduleOperation instanceof MetadataOperation) {
                // Process metadata operation
                MetadataOperation metadataOperation = (MetadataOperation) moduleOperation;
                // Save column names (row description)
                List<CString> columns = metadataOperation.getDataIds();
                parseStep.setColumns(columns);
                parseStep.setMetadata(true);
                // Save involved backends
                List<Integer> involvedBackends = metadataOperation.getInvolvedCSPs();
                if (involvedBackends == null) {
                    involvedBackends = Collections.emptyList();
                }
                parseStepStatus.setInvolvedBackends(involvedBackends);
                bindStepStatus.setInvolvedBackends(parseStepStatus.getInvolvedBackends());
                session.setCommandInvolvedBackends(bindStepStatus.getInvolvedBackends());
                // Forget SQL statement, reply directly to the frontend
                CommandResults commandResults = new CommandResults();
                commandResults.setBindCompleteRequired(true);
                result = Result.response(commandResults);
            } else if (moduleOperation instanceof OutboundDataOperation) {
                OutboundDataOperation outBoundDataOperation = (OutboundDataOperation) moduleOperation;
                // Save column names (row description)
                List<CString> columns = outBoundDataOperation.getDataIds().stream()
                        .map(id -> id.substring(id.lastIndexOf('/') + 1)).collect(Collectors.toList());
                parseStep.setColumns(columns);
                // Process data operation
                List<OutboundDataOperation> newOutboundDataOperations = newOutboundDataOperation(ctx,
                        outBoundDataOperation);
                if (stmt instanceof SetStatement || stmt instanceof StartTransaction || stmt instanceof Commit
                        || stmt instanceof CreateTable || stmt instanceof Alter || stmt instanceof CreateIndex
                        || stmt instanceof Drop || stmt instanceof Insert || stmt instanceof Select
                        || stmt instanceof DeclareCursor || stmt instanceof CursorFetch
                        || stmt instanceof CursorClose) {
                    if (newOutboundDataOperations.isEmpty()) {
                        // Forget SQL statement, reply directly to the frontend
                        CommandResults commandResults = new CommandResults();
                        commandResults.setBindCompleteRequired(true);
                        result = Result.response(commandResults);
                        parseStepStatus.setInvolvedBackends(Collections.emptyList());
                        bindStepStatus.setInvolvedBackends(parseStepStatus.getInvolvedBackends());
                        session.setCommandInvolvedBackends(bindStepStatus.getInvolvedBackends());
                        parseStep.setColumns(null);
                    } else {
                        List<Integer> involvedBackends;
                        List<ExpectedField> expectedFields = null;
                        boolean requestModified = newOutboundDataOperations.size() > 1
                                || newOutboundDataOperations.get(0).isModified();
                        if (FORCE_SQL_PROCESSING || requestModified) {
                            List<List<ExtendedQuery>> extendedQueries = new ArrayList<>(
                                    newOutboundDataOperations.size());
                            involvedBackends = new ArrayList<>(newOutboundDataOperations.size());
                            if (session.getCurrentCommandOperation() == Operation.READ) {
                                expectedFields = new ArrayList<>();
                            }
                            // Modify SQL statement
                            for (OutboundDataOperation newOutboundDataOperation : newOutboundDataOperations) {
                                PgsqlStatement<? extends Statement> newStatement;
                                String newSQL;
                                if (stmt instanceof SetStatement) {
                                    PgsqlStatement<SetStatement> statement = new PgsqlStatement<>((SetStatement) stmt,
                                            parseStep.getParameterTypes(), bindStep.getParameterFormats(),
                                            parameterValues, bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifySetStatement(ctx, statement, newOutboundDataOperation,
                                            newOutboundDataOperations.size() > 1);
                                    Statement newSetStatement = newStatement.getStatement();
                                    newSQL = newSetStatement.toString();
                                } else if (stmt instanceof StartTransaction) {
                                    PgsqlStatement<StartTransaction> statement = new PgsqlStatement<>(
                                            (StartTransaction) stmt, parseStep.getParameterTypes(),
                                            bindStep.getParameterFormats(), parameterValues,
                                            bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifyStartTransactionStatement(ctx, statement,
                                            newOutboundDataOperation, newOutboundDataOperations.size() > 1);
                                    Statement newStartTransaction = newStatement.getStatement();
                                    newSQL = newStartTransaction.toString();
                                } else if (stmt instanceof Commit) {
                                    PgsqlStatement<Commit> statement = new PgsqlStatement<>((Commit) stmt,
                                            parseStep.getParameterTypes(), bindStep.getParameterFormats(),
                                            parameterValues, bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifyCommitStatement(ctx, statement, newOutboundDataOperation,
                                            newOutboundDataOperations.size() > 1);
                                    Statement newCommit = newStatement.getStatement();
                                    newSQL = newCommit.toString();
                                } else if (stmt instanceof CreateTable) {
                                    PgsqlStatement<CreateTable> statement = new PgsqlStatement<>((CreateTable) stmt,
                                            parseStep.getParameterTypes(), bindStep.getParameterFormats(),
                                            parameterValues, bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifyCreateTableStatement(ctx, statement, newOutboundDataOperation,
                                            newOutboundDataOperations.size() > 1);
                                    Statement newCreateTable = newStatement.getStatement();
                                    newSQL = newCreateTable.toString();
                                } else if (stmt instanceof Alter) {
                                    PgsqlStatement<Alter> statement = new PgsqlStatement<>((Alter) stmt,
                                            parseStep.getParameterTypes(), bindStep.getParameterFormats(),
                                            parameterValues, bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifyAlterTableStatement(ctx, statement, newOutboundDataOperation,
                                            newOutboundDataOperations.size() > 1);
                                    Statement newAlter = newStatement.getStatement();
                                    newSQL = newAlter.toString();
                                } else if (stmt instanceof CreateIndex) {
                                    PgsqlStatement<CreateIndex> statement = new PgsqlStatement<>((CreateIndex) stmt,
                                            parseStep.getParameterTypes(), bindStep.getParameterFormats(),
                                            parameterValues, bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifyCreateIndexStatement(ctx, statement, newOutboundDataOperation,
                                            newOutboundDataOperations.size() > 1);
                                    Statement newCreateIndex = newStatement.getStatement();
                                    newSQL = newCreateIndex.toString();
                                } else if (stmt instanceof Drop) {
                                    PgsqlStatement<Drop> statement = new PgsqlStatement<>((Drop) stmt,
                                            parseStep.getParameterTypes(), bindStep.getParameterFormats(),
                                            parameterValues, bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifyDropTableStatement(ctx, statement, newOutboundDataOperation,
                                            newOutboundDataOperations.size() > 1);
                                    Statement newDropTable = newStatement.getStatement();
                                    newSQL = newDropTable.toString();
                                } else if (stmt instanceof Insert) {
                                    PgsqlStatement<Insert> statement = new PgsqlStatement<>((Insert) stmt,
                                            parseStep.getParameterTypes(), bindStep.getParameterFormats(),
                                            parameterValues, bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifyInsertStatement(ctx, statement, newOutboundDataOperation,
                                            newOutboundDataOperations.size() > 1, 0);
                                    Statement newInsert = newStatement.getStatement();
                                    newSQL = newInsert.toString();
                                } else if (stmt instanceof Select) {
                                    PgsqlStatement<Select> statement = new PgsqlStatement<>((Select) stmt,
                                            parseStep.getParameterTypes(), bindStep.getParameterFormats(),
                                            parameterValues, bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifySelectStatement(ctx, statement, newOutboundDataOperation,
                                            newOutboundDataOperations.size() > 1, expectedFields);
                                    Statement newSelect = newStatement.getStatement();
                                    newSQL = newSelect.toString();
                                } else if (stmt instanceof DeclareCursor) {
                                    PgsqlStatement<DeclareCursor> statement = new PgsqlStatement<>((DeclareCursor) stmt,
                                            parseStep.getParameterTypes(), bindStep.getParameterFormats(),
                                            parameterValues, bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifyDeclareCursorStatement(ctx, statement,
                                            newOutboundDataOperation, newOutboundDataOperations.size() > 1,
                                            expectedFields);
                                    Statement newDeclareCursor = newStatement.getStatement();
                                    newSQL = newDeclareCursor.toString();
                                } else if (stmt instanceof CursorFetch) {
                                    PgsqlStatement<CursorFetch> statement = new PgsqlStatement<>((CursorFetch) stmt,
                                            parseStep.getParameterTypes(), bindStep.getParameterFormats(),
                                            parameterValues, bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifyCursorFetchStatement(ctx, statement, newOutboundDataOperation,
                                            newOutboundDataOperations.size() > 1);
                                    Statement newCursorFetch = newStatement.getStatement();
                                    newSQL = newCursorFetch.toString();
                                } else if (stmt instanceof CursorClose) {
                                    PgsqlStatement<CursorClose> statement = new PgsqlStatement<>((CursorClose) stmt,
                                            parseStep.getParameterTypes(), bindStep.getParameterFormats(),
                                            parameterValues, bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newStatement = modifyCursorCloseStatement(ctx, statement, newOutboundDataOperation,
                                            newOutboundDataOperations.size() > 1);
                                    Statement newCursorClose = newStatement.getStatement();
                                    newSQL = newCursorClose.toString();
                                } else {
                                    newStatement = new PgsqlStatement<>(stmt, parseStep.getParameterTypes(),
                                            bindStep.getParameterFormats(), parameterValues,
                                            bindStep.getResultColumnFormats(), parseStep.getColumns());
                                    newSQL = stmt.toString();
                                }
                                newSQL = StringUtilities.addIrrelevantCharacters(newSQL, parseStep.getSQL(),
                                        " \t\r\n;");
                                parseStep = new ParseStep(parseStep.getName(), CString.valueOf(newSQL),
                                        parseStep.isMetadata(), newStatement.getColumns(),
                                        newStatement.getParameterTypes());
                                // get the parameter values
                                List<ByteBuf> newValues = newStatement.getParameterValues().stream()
                                        .map(ParameterValue::getValue)
                                        // build a list
                                        .collect(Collectors.toList());
                                bindStep = new BindStep(bindStep.getName(), bindStep.getPreparedStatement(),
                                        newStatement.getParameterFormats(), newValues, newStatement.getResultFormats());
                                int involvedBackend = newOutboundDataOperation.getInvolvedCSP();
                                if (involvedBackend == -1) {
                                    involvedBackend = getPreferredBackend(ctx);
                                }
                                involvedBackends.add(involvedBackend);
                                // Build result
                                List<ExtendedQuery> newQueries = new ArrayList<ExtendedQuery>();
                                if (!parseStepStatus.isProcessed()) {
                                    newQueries.add(parseStep);
                                }
                                if (describeStepStatus != null && !describeStepStatus.isProcessed()) {
                                    newQueries.add(describeStepStatus.getQuery());
                                }
                                if (!bindStepStatus.isProcessed()) {
                                    newQueries.add(bindStep);
                                }
                                if (extendedQueries.size() <= involvedBackend) {
                                    for (int i = extendedQueries.size(); i <= involvedBackend; i++) {
                                        extendedQueries.add(null);
                                    }
                                }
                                extendedQueries.set(involvedBackend, newQueries);
                            }
                            if (!parseStepStatus.isProcessed()) {
                                // ignore response
                                session.addLastQueryResponseToIgnore(QueryResponseType.PARSE_COMPLETE);
                                parseStepStatus.setProcessed(true);
                            }
                            if (describeStepStatus != null) {
                                // Set as current describe step
                                session.setCurrentDescribeStepStatus(describeStepStatus);
                                // ignore responses
                                session.addLastQueryResponseToIgnore(QueryResponseType.PARAMETER_DESCRIPTION);
                                session.addLastQueryResponseToIgnore(
                                        QueryResponseType.ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA);
                                describeStepStatus.setProcessed(true);
                            }
                            bindStepStatus.setProcessed(true);
                            result = Result.queries(extendedQueries);
                        } else {
                            involvedBackends = outBoundDataOperation.getInvolvedCSPs();
                            if (involvedBackends == null) {
                                involvedBackends = Collections.singletonList(getPreferredBackend(ctx));
                            }
                            if (session.getCurrentCommandOperation() == Operation.READ) {
                                @SuppressWarnings("unchecked")
                                List<Map.Entry<SelectItem, List<String>>> selectItemIds = (List<Map.Entry<SelectItem, List<String>>>) outBoundDataOperation
                                        .getAttribute("selectItemIds");
                                expectedFields = selectItemIds.stream().map(e -> {
                                    SelectItem item = e.getKey();
                                    String fqOutputName = toOutputName(item.toString());
                                    List<String> attributeNames = e.getValue();
                                    List<Map.Entry<String, Integer>> attributeMapping = IntStream
                                            .range(0, attributeNames.size())
                                            .mapToObj(i -> new SimpleEntry<>(attributeNames.get(i), i))
                                            .collect(Collectors.toList());
                                    Map<Integer, List<ExpectedProtectedField>> protectedFields = Stream
                                            .of(new ExpectedProtectedField(0, fqOutputName, attributeNames,
                                                    attributeMapping))
                                            .collect(Collectors.groupingBy(ExpectedProtectedField::getBackend));
                                    return new ExpectedField(fqOutputName, attributeNames, protectedFields);
                                }).collect(Collectors.toList());
                            }
                            // Build result
                            List<ExtendedQuery> newQueries = new ArrayList<ExtendedQuery>();
                            if (!parseStepStatus.isProcessed()) {
                                newQueries.add(parseStep);
                                // ignore response
                                session.addLastQueryResponseToIgnore(QueryResponseType.PARSE_COMPLETE);
                                parseStepStatus.setProcessed(true);
                            }
                            if (describeStepStatus != null) {
                                newQueries.add(describeStepStatus.getQuery());
                                // Set as current describe step status
                                session.setCurrentDescribeStepStatus(describeStepStatus);
                                // ignore responses
                                session.addLastQueryResponseToIgnore(QueryResponseType.PARAMETER_DESCRIPTION);
                                session.addLastQueryResponseToIgnore(
                                        QueryResponseType.ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA);
                                describeStepStatus.setProcessed(true);
                            }
                            if (!bindStepStatus.isProcessed()) {
                                newQueries.add(bindStep);
                                bindStepStatus.setProcessed(true);
                            }
                            result = Result.query(newQueries);
                        }
                        parseStepStatus.setInvolvedBackends(involvedBackends);
                        bindStepStatus.setInvolvedBackends(parseStepStatus.getInvolvedBackends());
                        session.setCommandInvolvedBackends(parseStepStatus.getInvolvedBackends());
                        if (session.getCurrentCommandOperation() == Operation.READ) {
                            session.setResultProcessingEnabled(
                                    requestModified || !outBoundDataOperation.isUnprotectingDataEnabled());
                            session.setPromise(newOutboundDataOperations.get(0).getPromise());
                            session.setUnprotectingDataEnabled(outBoundDataOperation.isUnprotectingDataEnabled());
                            session.setExpectedFields(expectedFields);
                            if (stmt instanceof DeclareCursor) {
                                // Save cursor context
                                session.saveCursorContext(((DeclareCursor) stmt).getName());
                            }
                        } else {
                            if (stmt instanceof CursorFetch) {
                                // Restore cursor context
                                session.restoreCursorContext(((CursorFetch) stmt).getName());
                            } else if (stmt instanceof CursorClose) {
                                // Remove cursor context
                                session.removeCursorContext(((CursorClose) stmt).getName());
                            }
                        }
                    }
                } else if (stmt instanceof Update) {
                    // TODO Update
                    // Build result
                    List<ExtendedQuery> newQueries = new ArrayList<ExtendedQuery>();
                    if (!parseStepStatus.isProcessed()) {
                        newQueries.add(parseStep);
                        // ignore response
                        session.addLastQueryResponseToIgnore(QueryResponseType.PARSE_COMPLETE);
                        parseStepStatus.setProcessed(true);
                    }
                    if (describeStepStatus != null) {
                        newQueries.add(describeStepStatus.getQuery());
                        // Set as current describe step status
                        session.setCurrentDescribeStepStatus(describeStepStatus);
                        // ignore responses
                        session.addLastQueryResponseToIgnore(QueryResponseType.PARAMETER_DESCRIPTION);
                        session.addLastQueryResponseToIgnore(QueryResponseType.ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA);
                        describeStepStatus.setProcessed(true);
                    }
                    if (!bindStepStatus.isProcessed()) {
                        newQueries.add(bindStep);
                        bindStepStatus.setProcessed(true);
                    }
                    result = Result.query(newQueries);
                } else if (stmt instanceof Delete) {
                    // TODO Delete
                    // Build result
                    List<ExtendedQuery> newQueries = new ArrayList<ExtendedQuery>();
                    if (!parseStepStatus.isProcessed()) {
                        newQueries.add(parseStep);
                        // ignore response
                        session.addLastQueryResponseToIgnore(QueryResponseType.PARSE_COMPLETE);
                        parseStepStatus.setProcessed(true);
                    }
                    if (describeStepStatus != null) {
                        newQueries.add(describeStepStatus.getQuery());
                        // Set as current describe step status
                        session.setCurrentDescribeStepStatus(describeStepStatus);
                        // ignore responses
                        session.addLastQueryResponseToIgnore(QueryResponseType.PARAMETER_DESCRIPTION);
                        session.addLastQueryResponseToIgnore(QueryResponseType.ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA);
                        describeStepStatus.setProcessed(true);
                    }
                    if (!bindStepStatus.isProcessed()) {
                        newQueries.add(bindStep);
                        bindStepStatus.setProcessed(true);
                    }
                    result = Result.query(newQueries);
                }
            }
        }
        if (result == null) {
            // Build result
            List<ExtendedQuery> newQueries = new ArrayList<ExtendedQuery>();
            if (!parseStepStatus.isProcessed()) {
                newQueries.add(parseStep);
                // ignore response
                session.addLastQueryResponseToIgnore(QueryResponseType.PARSE_COMPLETE);
                parseStepStatus.setProcessed(true);
            }
            if (describeStepStatus != null) {
                newQueries.add(describeStepStatus.getQuery());
                // Set as current describe step status
                session.setCurrentDescribeStepStatus(describeStepStatus);
                // ignore responses
                session.addLastQueryResponseToIgnore(QueryResponseType.PARAMETER_DESCRIPTION);
                session.addLastQueryResponseToIgnore(QueryResponseType.ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA);
                describeStepStatus.setProcessed(true);
            }
            if (!bindStepStatus.isProcessed()) {
                newQueries.add(bindStep);
                bindStepStatus.setProcessed(true);
            }
            result = Result.query(newQueries);
        }
        return result;
    }

    private Statement parseSQL(ChannelHandlerContext ctx, CString sql) {
        // Parse statement
        Statement stmt = null;
        ByteBuf byteBuf = null;
        try {
            if (sql.isBuffered()) {
                byteBuf = sql.getByteBuf();
                byteBuf.markReaderIndex();
                stmt = CCJSqlParserUtil.parse(new ByteBufInputStream(byteBuf.readSlice(sql.length())),
                        StandardCharsets.ISO_8859_1.name());
            } else {
                stmt = CCJSqlParserUtil.parse(sql.toString());
            }
        } catch (JSQLParserException | TokenMgrError e) {
            if (byteBuf != null) {
                byteBuf.resetReaderIndex();
            }
            LOGGER.error("Parsing error for {} : ", sql);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Parsing error details:", e);
            }
        }
        return stmt;
    }

    private Map<Byte, CString> bufferQuery(ChannelHandlerContext ctx, Query query) {
        Map<Byte, CString> errorDetails = null;
        SQLSession session = getSession(ctx);
        session.addBufferedQuery(query);
        if (query instanceof SimpleQuery) {
            if (session.getTransactionStatus() == (byte) 'E') {
                if (session
                        .lastQueryResponseToIgnore() == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                    // command ignored, don't expect to receive command complete
                    // response
                    // don't notify frontend of error
                } else {
                    // expect to receive error response
                    session.addLastQueryResponseToIgnore(
                            QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR);
                }
                // notify frontend of error
                errorDetails = session.getRetainedTransactionErrorDetails();
            } else {
                session.addLastQueryResponseToIgnore(
                        QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR);
            }
        } else {
            ExtendedQuery extendedQuery = (ExtendedQuery) query;
            if (session.getTransactionStatus() == (byte) 'E') {
                if (extendedQuery instanceof ParseStep) {
                    // expect to receive error response
                    session.addLastQueryResponseToIgnore(
                            QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR);
                    // notify frontend of error
                    errorDetails = session.getRetainedTransactionErrorDetails();
                } else if (extendedQuery instanceof BindStep) {
                    if (session
                            .lastQueryResponseToIgnore() == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                        // command ignored, don't expect to receive bind
                        // complete response
                        // don't notify frontend of error
                    } else {
                        // expect to receive error response
                        session.addLastQueryResponseToIgnore(
                                QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR);
                        // notify frontend of error
                        errorDetails = session.getRetainedTransactionErrorDetails();
                    }
                } else if (extendedQuery instanceof DescribeStep) {
                    if (session
                            .lastQueryResponseToIgnore() == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                        // command ignored, don't expect to receive parameter
                        // description, row description or no data responses
                        // don't notify frontend of error
                    } else {
                        // expect to receive error response
                        session.addLastQueryResponseToIgnore(
                                QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR);
                        // notify frontend of error
                        errorDetails = session.getRetainedTransactionErrorDetails();
                    }
                } else if (extendedQuery instanceof ExecuteStep) {
                    if (session
                            .lastQueryResponseToIgnore() == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                        // command ignored, don't expect to receive command
                        // complete response
                        // don't notify frontend of error
                    } else {
                        // expect to receive error response
                        session.addLastQueryResponseToIgnore(
                                QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR);
                        // notify frontend of error
                        errorDetails = session.getRetainedTransactionErrorDetails();
                    }
                } else if (extendedQuery instanceof CloseStep) {
                    if (session
                            .lastQueryResponseToIgnore() == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                        // command ignored, don't expect to receive close
                        // complete response
                        // don't notify frontend of error
                    } else {
                        // expect to receive error response
                        session.addLastQueryResponseToIgnore(
                                QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR);
                        // notify frontend of error
                        errorDetails = session.getRetainedTransactionErrorDetails();
                    }
                } else if (extendedQuery instanceof SynchronizeStep) {
                    // expect to receive ready for query response after error
                    // response
                    session.addLastQueryResponseToIgnore(QueryResponseType.READY_FOR_QUERY);
                    // don't notify frontend of error
                } else if (extendedQuery instanceof FlushStep) {
                    // don't expect to receive any response
                    if (session
                            .lastQueryResponseToIgnore() == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                        // don't notify frontend of error
                    } else {
                        // notify frontend of error
                        errorDetails = session.getRetainedTransactionErrorDetails();
                    }
                }
            } else {
                if (extendedQuery instanceof ParseStep) {
                    session.addLastQueryResponseToIgnore(QueryResponseType.PARSE_COMPLETE);
                } else if (extendedQuery instanceof BindStep) {
                    session.addLastQueryResponseToIgnore(QueryResponseType.BIND_COMPLETE);
                } else if (extendedQuery instanceof DescribeStep) {
                    if (((DescribeStep) query).getCode() == 'S') {
                        session.addLastQueryResponseToIgnore(QueryResponseType.PARAMETER_DESCRIPTION);
                    }
                    session.addLastQueryResponseToIgnore(QueryResponseType.ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA);
                } else if (extendedQuery instanceof ExecuteStep) {
                    session.addLastQueryResponseToIgnore(
                            QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR);
                } else if (extendedQuery instanceof CloseStep) {
                    session.addLastQueryResponseToIgnore(QueryResponseType.CLOSE_COMPLETE);
                } else if (extendedQuery instanceof SynchronizeStep) {
                    session.addLastQueryResponseToIgnore(QueryResponseType.READY_FOR_QUERY);
                } else if (extendedQuery instanceof FlushStep) {
                    // don't expect to receive any response
                }
            }
        }
        return errorDetails;
    }

    @Override
    public MessageTransferMode<Void, Void> processParseCompleteResponse(ChannelHandlerContext ctx) {
        LOGGER.debug("Parse complete");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        // Note: use Boolean instead of Void because insertion of null is not
        // permitted
        SortedMap<Integer, Boolean> all = session.newQueryResponse(QueryResponseType.PARSE_COMPLETE, backend,
                numberOfBackends, Boolean.TRUE);
        if (all == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            // All backends have replied. Merge responses
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            if (nextQueryResponseToIgnore == null) {
                transferMode = TransferMode.FORWARD;
            } else if (nextQueryResponseToIgnore == QueryResponseType.PARSE_COMPLETE) {
                transferMode = TransferMode.FORGET;
                session.removeFirstQueryResponseToIgnore();
            } else {
                throw new IllegalStateException(String.format("Unexpected %s response (%s or null was expected)",
                        nextQueryResponseToIgnore, QueryResponseType.PARSE_COMPLETE));
            }
        }
        MessageTransferMode<Void, Void> mode = new MessageTransferMode<>(transferMode, null);
        LOGGER.debug("Parse complete processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Void, Void> processBindCompleteResponse(ChannelHandlerContext ctx) {
        LOGGER.debug("Bind complete");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        // Note: use Boolean instead of Void because insertion of null is not
        // permitted
        SortedMap<Integer, Boolean> all = session.newQueryResponse(QueryResponseType.BIND_COMPLETE, backend,
                numberOfBackends, Boolean.TRUE);
        if (all == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            // All backends have replied. Merge responses
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            if (nextQueryResponseToIgnore == null || nextQueryResponseToIgnore == QueryResponseType.PARSE_COMPLETE) {
                transferMode = TransferMode.FORWARD;
            } else if (nextQueryResponseToIgnore == QueryResponseType.BIND_COMPLETE) {
                transferMode = TransferMode.FORGET;
                session.removeFirstQueryResponseToIgnore();
            } else {
                throw new IllegalStateException(String.format("Unexpected %s response (%s or null was expected)",
                        nextQueryResponseToIgnore, QueryResponseType.BIND_COMPLETE));
            }
        }
        MessageTransferMode<Void, Void> mode = new MessageTransferMode<>(transferMode, null);
        LOGGER.debug("Bind complete processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<List<Long>, Void> processParameterDescriptionResponse(ChannelHandlerContext ctx,
            List<Long> types) {
        LOGGER.debug("Parameter description: {}", types);
        TransferMode transferMode;
        List<Long> newTypes = null;
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        SortedMap<Integer, List<Long>> allTypes = session.newQueryResponse(QueryResponseType.PARAMETER_DESCRIPTION,
                backend, numberOfBackends, types);
        if (allTypes == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            // All backends have replied. Merge responses
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            if (nextQueryResponseToIgnore == null) {
                transferMode = TransferMode.FORWARD;
                if (numberOfBackends == 1) {
                    newTypes = types;
                } else {
                    newTypes = allTypes.values().stream().flatMap(List::stream).collect(Collectors.toList());
                }
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("{} new type(s) replace {} old type(s) from {} backend(s)", newTypes.size(),
                            allTypes.values().stream().flatMap(List::stream).count(), numberOfBackends);
                }
                if (newTypes == types || newTypes.stream().allMatch(nt -> types.stream().anyMatch(t -> t == nt))) {
                    LOGGER.trace("new types from one backend");
                } else if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("at list one type is new or is from another backend");
                    newTypes.stream().filter(nt -> types.stream().noneMatch(t -> t == nt)).forEach(nt -> {
                        LOGGER.trace("type {} is new or is from another backend", nt);
                    });
                }
            } else if (nextQueryResponseToIgnore == QueryResponseType.PARAMETER_DESCRIPTION) {
                transferMode = TransferMode.FORGET;
                session.removeFirstQueryResponseToIgnore();
            } else {
                throw new IllegalStateException(String.format("Unexpected %s response (%s or null was expected)",
                        nextQueryResponseToIgnore, QueryResponseType.PARAMETER_DESCRIPTION));
            }
        }
        MessageTransferMode<List<Long>, Void> mode = new MessageTransferMode<>(transferMode,
                Collections.singletonList(newTypes));
        LOGGER.debug("Parameter description processed: new types={}, transfer mode={}", mode.getNewContent(),
                mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>, Void> processRowDescriptionResponse(
            ChannelHandlerContext ctx, List<PgsqlRowDescriptionMessage.Field> fields) {
        LOGGER.debug("Row description: {}", fields);
        TransferMode transferMode;
        List<PgsqlRowDescriptionMessage.Field> newFields = null;
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        SortedMap<Integer, List<PgsqlRowDescriptionMessage.Field>> allFields = session
                .newQueryResponse(QueryResponseType.ROW_DESCRIPTION, backend, numberOfBackends, fields);
        if (allFields == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            // All backends have replied. Merge responses
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            if (nextQueryResponseToIgnore == null || nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY) {
                transferMode = TransferMode.FORWARD;
                if (numberOfBackends == 1) {
                    newFields = fields;
                } else {
                    newFields = allFields.values().stream().flatMap(List::stream).collect(Collectors.toList());
                }
                if (session.getCurrentCommandOperation() == Operation.READ) {
                    if (FORCE_SQL_PROCESSING || session.isResultProcessingEnabled()) {
                        if (session.getExpectedFields() != null) {
                            newFields = processRowDescription(ctx, allFields, involvedBackends);
                        }
                    }
                    session.setBackendRowDescriptions(allFields);
                    session.setRowDescription(newFields);
                    if (session.getCurrentDescribeStepStatus() != null) {
                        session.getCurrentDescribeStepStatus().setExpectedFields(session.getExpectedFields());
                        session.getCurrentDescribeStepStatus().setBackendRowDescriptions(allFields);
                        session.getCurrentDescribeStepStatus().setRowDescription(newFields);
                    }
                    Map<Integer, List<Integer>> joinFieldIndexes = processRowJoinFields(ctx, allFields,
                            session.getExpectedFields());
                    session.setJoinFieldIndexes(joinFieldIndexes);
                }
                // Release buffers if necessary
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("{} new field(s) replace {} old field(s) from {} backend(s)", newFields.size(),
                            allFields.values().stream().flatMap(List::stream).count(), numberOfBackends);
                }
                if (newFields == fields || newFields.stream().allMatch(nf -> fields.stream().anyMatch(f -> f == nf))) {
                    LOGGER.trace("new fields from one backend");
                    // release the initial buffers that has been retained by
                    // session.addQueryResponse (avoid memory leaks)
                    newFields.stream().forEach(nf -> {
                        if (nf.release() && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("field {} deallocated", nf);
                        }
                    });
                } else if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("at list one field is new or is from another backend");
                    newFields.stream().filter(nf -> fields.stream().noneMatch(f -> f == nf)).forEach(nf -> {
                        LOGGER.trace("field {} is new or is from another backend", nf);
                    });
                }
                // release buffers of unused fields
                List<PgsqlRowDescriptionMessage.Field> nfs = newFields;
                allFields.values().stream().flatMap(List::stream).forEach(f -> {
                    if (nfs.stream().noneMatch(nf -> nf == f)) {
                        if (f.release() && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("field {} deallocated", f);
                        }
                    }
                });
            } else if (nextQueryResponseToIgnore == QueryResponseType.ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA) {
                transferMode = TransferMode.FORGET;
                session.removeFirstQueryResponseToIgnore();
            } else {
                throw new IllegalStateException(String.format("Unexpected %s response (%s or null was expected)",
                        nextQueryResponseToIgnore, QueryResponseType.ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA));
            }
        }
        MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>, Void> mode = new MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>, Void>(
                transferMode, newFields);
        LOGGER.debug("Row description processed: new fields={}, transfer mode={}", mode.getNewContent(),
                mode.getTransferMode());
        return mode;
    }

    private List<PgsqlRowDescriptionMessage.Field> processRowDescription(ChannelHandlerContext ctx,
            SortedMap<Integer, List<PgsqlRowDescriptionMessage.Field>> allFields, List<Integer> involvedBackends) {
        SQLSession session = getSession(ctx);
        int maxBackend = involvedBackends.stream().max(Comparator.naturalOrder()).get();
        // check if new field list (by modification or by merging) is necessary
        boolean newList = FORCE_SQL_PROCESSING || allFields.size() > 1 || !session.isUnprotectingDataEnabled();
        if (!newList) {
            List<ExpectedField> expectedFields = session.getExpectedFields();
            newList = expectedFields != null
                    && expectedFields.stream()
                            .anyMatch(
                                    ((Predicate<ExpectedField>) ef -> ef.getProtectedFields().size() != 1)
                                            .or(ef -> ef.getProtectedFields().size() > 0
                                                    && ef.getProtectedFields().get(involvedBackends.get(0)).size() > 0
                                                    && (!ef.getProtectedFields().get(involvedBackends.get(0)).get(0)
                                                            .getName().equals(ef.getName())
                                                            || ef.getName().equals("*"))));
        }
        List<ExpectedField> expectedFields = session.getExpectedFields();
        // modify (and merge) fields, preserving by their position in the
        // original query (or by their natural order in case of asterisks)
        // for each expected field, for each expected protected field (per
        // backend):
        // - replace asterisk by real name in protected field name
        // - replace asterisks by real names in protected attribute names
        // - remove protected attribute names that are not pertinent
        // - map protected field name and protected attribute names to their
        // field position
        int[] positionPerBackend = new int[maxBackend + 1];
        Arrays.fill(positionPerBackend, 0);
        expectedFields.stream().map(ExpectedField::getProtectedFields).forEach(protectedFields -> {
            allFields.entrySet().forEach(entry -> {
                int backend = entry.getKey();
                List<ExpectedProtectedField> backendProtectedFields = protectedFields.get(backend);
                if (backendProtectedFields == null) {
                    return;
                }
                List<Field> backendFields = entry.getValue();
                for (int i = 0; i < backendProtectedFields.size(); i++) {
                    ExpectedProtectedField protectedField = backendProtectedFields.get(i);
                    String protectedName = protectedField.getName();
                    String protectedFieldname = toOutputName(protectedName);
                    if (protectedFieldname.equals("*")) {
                        backendProtectedFields.remove(i);
                        int tableOID = backendFields.get(positionPerBackend[backend]).getTableOID();
                        List<Short> columnNumbers = new ArrayList<>(backendFields.size() - positionPerBackend[backend]);
                        while (positionPerBackend[backend] < backendFields.size()) {
                            PgsqlRowDescriptionMessage.Field backendField = backendFields
                                    .get(positionPerBackend[backend]);
                            if (backendField.getTableOID() != tableOID) {
                                break;
                            }
                            short columnNumber = backendField.getColumnNumber();
                            if (columnNumbers.contains(columnNumber)) {
                                break;
                            }
                            columnNumbers.add(columnNumber);
                            String newFieldName = protectedName.replace("*", backendField.getName().toString());
                            List<Map.Entry<String, Integer>> protectedAttributes = protectedField.getAttributes();
                            List<Map.Entry<String, Integer>> newProtectedAttributes = protectedAttributes.stream()
                                    .map(e -> toOutputName(e.getKey()).equals("*") ? new SimpleEntry<>(
                                            e.getKey().replace("*", backendField.getName().toString()), 0) : e)
                                    .filter(e -> backendField.getName().equals(toOutputName(e.getKey())))
                                    .peek(e -> e.setValue(positionPerBackend[backend])).collect(Collectors.toList());
                            if (newProtectedAttributes.isEmpty()) {
                                LOGGER.trace("strange... unexpected protected attribute size");
                                newProtectedAttributes = Stream.of(new SimpleEntry<>(backendField.getName().toString(),
                                        positionPerBackend[backend])).collect(Collectors.toList());
                            }
                            List<Map.Entry<String, Integer>> newProtectedAttributes2 = newProtectedAttributes;
                            List<Map.Entry<String, Integer>> newAttributeMapping = protectedField.getAttributeMapping()
                                    .stream()
                                    .map(e -> toOutputName(e.getKey()).equals("*") ? new SimpleEntry<>(
                                            e.getKey().replace("*", backendField.getName().toString()), 0) : e)
                                    .filter(e -> newProtectedAttributes2.stream()
                                            .anyMatch(e2 -> e2.getKey().equals(e.getKey())))
                                    .collect(Collectors.toList());
                            ExpectedProtectedField newProtectedField = new ExpectedProtectedField(backend, newFieldName,
                                    positionPerBackend[backend], newProtectedAttributes, newAttributeMapping);
                            backendProtectedFields.add(i, newProtectedField);
                            i++;
                            positionPerBackend[backend]++;
                        }
                        i--;
                    } else {
                        PgsqlRowDescriptionMessage.Field backendField = backendFields.get(positionPerBackend[backend]);
                        if (!backendField.getName().equals(protectedFieldname)) {
                            if (!backendField.getName().equals("?column?")) {
                                LOGGER.trace("strange... unexpected protected field name");
                            }
                            protectedField.setName(backendField.getName().toString());
                        }
                        protectedField.setPosition(positionPerBackend[backend]);
                        protectedField.getAttributes().forEach(e -> e.setValue(positionPerBackend[backend]));
                        positionPerBackend[backend]++;
                    }
                    if (positionPerBackend[backend] > backendFields.size()) {
                        throw new IllegalStateException("unexpected");
                    }
                }
            });
        });
        // verify positions are ok in expected protected fields
        Arrays.fill(positionPerBackend, -1);
        int[] nbFieldsPerBackend = new int[maxBackend + 1];
        for (int backend = 0; backend < nbFieldsPerBackend.length; backend++) {
            List<PgsqlRowDescriptionMessage.Field> backendFields = allFields.get(backend);
            nbFieldsPerBackend[backend] = backendFields != null ? backendFields.size() : 0;
        }
        expectedFields.forEach(ef -> {
            ef.getProtectedFields().entrySet().stream().forEach(entry -> {
                int backend = entry.getKey();
                List<ExpectedProtectedField> backendProtectedFields = entry.getValue();
                if (!ef.getName().equals("*") && backendProtectedFields.size() > 1) {
                    throw new IllegalStateException("unexpected");
                }
                backendProtectedFields.forEach(epf -> {
                    Integer position = epf.getPosition();
                    if (position == null || position < 0 || position >= nbFieldsPerBackend[backend]
                            || position - positionPerBackend[backend] != 1) {
                        throw new IllegalStateException("unexpected");
                    }
                    epf.getAttributes().stream().map(Map.Entry::getValue).forEach(pos -> {
                        if (pos == null || pos < 0 || pos >= nbFieldsPerBackend[backend]
                                || pos - positionPerBackend[backend] < 0 || pos - positionPerBackend[backend] > 1) {
                            throw new IllegalStateException("unexpected");
                        }
                    });
                    positionPerBackend[backend] = position;
                });
            });
        });
        // prepare reverse mapping of attributes
        List<DataOperationCommand> promise = session.getPromise();
        List<Map<String, String>> attributeMapping = promise != null
                ? promise.stream().map(doc -> doc != null ? doc.getMapping() : null).collect(Collectors.toList())
                : null;
        List<Map<String, String>> reversedAttributeMapping = attributeMapping != null
                ? attributeMapping.stream()
                        .map(map -> map != null ? map.entrySet().stream()
                                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)) : null)
                        .collect(Collectors.toList())
                : null;
        // build new field list
        List<Map.Entry<String, List<PgsqlRowDescriptionMessage.Field>>> expectedFieldToRowDescription;
        List<PgsqlRowDescriptionMessage.Field> newFields;
        if (newList) {
            // Track backend of type OIDs
            SQLDatabaseSchema databaseSchema = getDatabaseSchema(ctx);
            allFields.entrySet().forEach(e -> e.getValue().forEach(f -> {
                databaseSchema.addTypeOIDBackend(f.getTypeOID(), e.getKey());
            }));
            // map expected clear field names to the fields, modifying their
            // name if necessary
            expectedFieldToRowDescription = expectedFields.stream().map(ef -> {
                String clearFieldName = ef.getName();
                return new SimpleEntry<>(clearFieldName, ef.getProtectedFields().entrySet().stream().flatMap(e -> {
                    int backend = e.getKey();
                    List<ExpectedProtectedField> protectedFields = e.getValue();
                    List<PgsqlRowDescriptionMessage.Field> backendFields = allFields.get(backend);
                    return protectedFields.stream().map(epf -> {
                        int position = epf.getPosition();
                        PgsqlRowDescriptionMessage.Field backendField = backendFields.get(position);
                        if (session.isUnprotectingDataEnabled()) {
                            if (!backendField.getName().equals("?column?")) {
                                String outputName;
                                if (clearFieldName.equals("*")) {
                                    outputName = reversedAttributeMapping != null ? reversedAttributeMapping.stream()
                                            .filter(map -> map != null).map(Map::entrySet).flatMap(Set::stream)
                                            .filter(e2 -> toOutputName(e2.getKey()).equals(epf.getName())).findAny()
                                            .map(Map.Entry::getValue).map(this::toOutputName).orElse(epf.getName())
                                            : epf.getName();
                                } else {
                                    outputName = clearFieldName;
                                }
                                if (!backendField.getName().equals(outputName)) {
                                    backendField = new PgsqlRowDescriptionMessage.Field(CString.valueOf(outputName),
                                            backendField.getTableOID(), backendField.getColumnNumber(),
                                            backendField.getTypeOID(), backendField.getTypeSize(),
                                            backendField.getTypeModifier(), backendField.getFormat());
                                }
                            }
                        } else {
                            CString backendFieldName = backendField.getName();
                            String protectedAttributeName = epf.getAttributes().stream().map(Map.Entry::getKey)
                                    .filter(pan -> backendFieldName.equals(toOutputName(pan))).findFirst()
                                    .orElse(epf.getAttributes().get(0).getKey());
                            backendField = new PgsqlRowDescriptionMessage.Field(
                                    CString.valueOf("csp" + (backend + 1) + "/" + protectedAttributeName).replace('/',
                                            '.'),
                                    backendField.getTableOID(), backendField.getColumnNumber(),
                                    backendField.getTypeOID(), backendField.getTypeSize(),
                                    backendField.getTypeModifier(), backendField.getFormat());
                        }
                        return backendField;
                    });
                }).collect(Collectors.toList()));
            }).collect(Collectors.toList());
            // merge the duplicate fields if necessary
            expectedFieldToRowDescription.stream().forEach(e -> {
                String clearFieldname = e.getKey();
                List<PgsqlRowDescriptionMessage.Field> fields = e.getValue();
                Stream<PgsqlRowDescriptionMessage.Field> stream;
                if (clearFieldname.equals("*")) {
                    stream = fields.stream();
                    if (session.isUnprotectingDataEnabled()) {
                        stream = stream.distinct()
                                .sorted(Comparator.comparing(PgsqlRowDescriptionMessage.Field::getColumnNumber));
                    }
                    // harmonize table oid and rebuild column number
                    stream = stream.map(new java.util.function.Function<Field, Field>() {
                        private short previous = -1;

                        @Override
                        public Field apply(Field field) {
                            if (field.getColumnNumber() <= previous) {
                                short newColumnNumber = (short) (previous + 1);
                                field = new PgsqlRowDescriptionMessage.Field((CString) field.getName().clone(),
                                        field.getTableOID(), newColumnNumber, field.getTypeOID(), field.getTypeSize(),
                                        field.getTypeModifier(), field.getFormat());
                            }
                            previous = field.getColumnNumber();
                            return field;
                        }
                    });
                } else {
                    if (session.isUnprotectingDataEnabled()) {
                        stream = fields.stream().limit(1);
                    } else {
                        stream = fields.stream();
                    }
                }
                fields = stream.collect(Collectors.toList());
                e.setValue(fields);
            });
            // build new list of fields
            newFields = expectedFieldToRowDescription.stream().map(Map.Entry::getValue).flatMap(List::stream)
                    .collect(Collectors.toList());
            if (!session.isUnprotectingDataEnabled()) {
                newFields = newFields.stream()
                        .sorted(Comparator
                                .comparing(f -> f.getName().substring(0, f.getName().indexOf('.')).toString()))
                        .collect(Collectors.toList());
            }
        } else {
            // no modification
            expectedFieldToRowDescription = expectedFields.stream()
                    .map(ef -> new SimpleEntry<>(ef.getName(),
                            ef.getProtectedFields().get(involvedBackends.get(0)).stream()
                                    .map(epf -> allFields.get(involvedBackends.get(0)).get(epf.getPosition()))
                                    .collect(Collectors.toList())))
                    .collect(Collectors.toList());
            newFields = allFields.get(involvedBackends.get(0));
        }
        int nbNewFields = newFields.size();
        // for each expected field:
        // - replace asterisk by real name in field name
        // - replace asterisks by real names in attribute names
        // - remove attribute names that are not pertinent
        // - map field name and attribute names to their field position
        for (int i = 0, j = 0, k = 0; i < expectedFields.size(); i++, j++, k++) {
            ExpectedField expectedField = expectedFields.get(i);
            if (expectedField.getProtectedFields().isEmpty()) {
                k--;
                continue;
            }
            String clearName = expectedField.getName();
            String clearFieldname = toOutputName(clearName);
            if (clearFieldname.equals("*")) {
                expectedFields.remove(i);
                List<Field> expectedFieldNewFields = expectedFieldToRowDescription.get(j).getValue();
                for (PgsqlRowDescriptionMessage.Field newField : expectedFieldNewFields) {
                    String newFieldName = toOutputName(newField.getName().toString());
                    int position = k;
                    List<Map.Entry<String, Integer>> newClearAttributes = expectedField.getAttributes().stream()
                            .map(e -> toOutputName(e.getKey()).equals("*")
                                    ? new SimpleEntry<>(e.getKey().replace("*", newFieldName), 0) : e)
                            .filter(e -> newFieldName.equals(toOutputName(e.getKey()))).map(Map.Entry::getKey)
                            .map(an -> new SimpleEntry<>(an, 0)).peek(e -> e.setValue(position))
                            .collect(Collectors.toList());
                    if (newClearAttributes.isEmpty()) {
                        LOGGER.trace("strange... unexpected attribute size");
                        newClearAttributes.add(new SimpleEntry<>(newFieldName, k));
                    }
                    String newClearFieldName = newClearAttributes.stream().findAny().map(Map.Entry::getKey)
                            .map(this::toOutputName).get();
                    String newClearName = clearName.replace("*", newClearFieldName);
                    // build list of protected field names by backend (from the
                    // attribute mapping)
                    List<List<String>> protectedFieldNames;
                    if (attributeMapping != null) {
                        protectedFieldNames = attributeMapping.stream()
                                .map(map -> map != null ? map.entrySet().stream()
                                        .filter(e -> newClearAttributes.stream()
                                                .anyMatch(e2 -> e.getKey().equals(e2.getKey())))
                                        .map(Map.Entry::getValue).map(this::toOutputName).distinct()
                                        .collect(Collectors.toList()) : null)
                                .collect(Collectors.toList());
                        if (protectedFieldNames.stream().filter(l -> l != null).allMatch(List::isEmpty)) {
                            protectedFieldNames.stream().filter(l -> l != null).forEach(l -> l.add(newFieldName));
                        }
                    } else {
                        protectedFieldNames = Collections.singletonList(Collections.singletonList(newFieldName));
                    }
                    Map<Integer, List<ExpectedProtectedField>> newProtectedFields = expectedField.getProtectedFields()
                            .entrySet().stream().filter(e -> e.getValue().stream().map(ExpectedProtectedField::getName)
                                    .map(n -> toOutputName(n)).anyMatch(n -> {
                                        List<String> backendProtectedFieldNames = protectedFieldNames.get(e.getKey());
                                        return backendProtectedFieldNames != null
                                                && backendProtectedFieldNames.contains(n);
                                    }))
                            .map(e -> new SimpleEntry<>(e.getKey(), e.getValue().stream().filter(epf -> {
                                List<String> backendProtectedFieldNames = protectedFieldNames.get(e.getKey());
                                return backendProtectedFieldNames != null
                                        && backendProtectedFieldNames.contains(toOutputName(epf.getName()));
                            }).collect(Collectors.toList())))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    // remove attributes (and mapping) in new protected fields
                    // for which referenced attribute name has been removed
                    newProtectedFields.values().stream().flatMap(List::stream).forEach(epf -> {
                        epf.getAttributes().removeIf(e -> {
                            return epf.getAttributeMapping().stream().filter(e2 -> e2.getKey().equals(e.getKey()))
                                    .map(Map.Entry::getValue).map(idx -> expectedField.getAttributes().get(idx))
                                    .map(Map.Entry::getKey)
                                    .map(an -> toOutputName(an).equals("*") ? an.replace("*", newClearFieldName) : an)
                                    .noneMatch(an -> newClearAttributes.stream().map(Map.Entry::getKey)
                                            .anyMatch(nan -> nan.equals(an)));
                        });
                        epf.getAttributeMapping().removeIf(e -> {
                            String an = expectedField.getAttributes().get(e.getValue()).getKey();
                            if (toOutputName(an).equals("*")) {
                                an = an.replace("*", newClearFieldName);
                            }
                            String an2 = an;
                            return newClearAttributes.stream().map(Map.Entry::getKey).noneMatch(nan -> nan.equals(an2));
                        });
                    });
                    ExpectedField newClearField = new ExpectedField(newClearName, k, newClearAttributes,
                            newProtectedFields);
                    expectedFields.add(i, newClearField);
                    i++;
                    k++;
                }
                i--;
                k--;
            } else {
                PgsqlRowDescriptionMessage.Field newField = expectedFieldToRowDescription.get(j).getValue().get(0);
                if (!newField.getName().equals(clearFieldname)) {
                    if (session.isUnprotectingDataEnabled() && !newField.getName().equals("?column?")) {
                        LOGGER.trace("strange... unexpected protected field name");
                    }
                    expectedField.setName(newField.getName().toString());
                }
                expectedField.setPosition(k);
                int position = k;
                expectedField.getAttributes().forEach(e -> e.setValue(position));
                // remove attributes (and mapping) in protected fields
                // for which referenced attribute name has been removed
                List<String> clearAttributeNames = expectedField.getAttributes().stream().map(Map.Entry::getKey)
                        .collect(Collectors.toList());
                expectedField.getProtectedFields().values().stream().flatMap(List::stream).forEach(epf -> {
                    epf.getAttributes().removeIf(e -> {
                        return epf.getAttributeMapping().stream().filter(e2 -> e2.getKey().equals(e.getKey()))
                                .map(Map.Entry::getValue).map(idx -> expectedField.getAttributes().get(idx))
                                .map(Map.Entry::getKey)
                                .noneMatch(an -> clearAttributeNames.stream().anyMatch(nan -> nan.equals(an)));
                    });
                    epf.getAttributeMapping().removeIf(e -> {
                        String an = expectedField.getAttributes().get(e.getValue()).getKey();
                        return clearAttributeNames.stream().noneMatch(nan -> nan.equals(an));
                    });
                });
            }
            if (k >= nbNewFields) {
                throw new IllegalStateException("unexpected");
            }
        }
        // verify positions are ok in expected fields
        positionPerBackend[0] = -1;
        expectedFields.forEach(ef -> {
            Integer position = ef.getPosition();
            if (ef.getProtectedFields().isEmpty()) {
                if (position != -1) {
                    throw new IllegalStateException("unexpected");
                }
                ef.getAttributes().stream().map(Map.Entry::getValue).forEach(pos -> {
                    if (pos != -1) {
                        throw new IllegalStateException("unexpected");
                    }
                });
            } else {
                if (position == null || position < 0 || position >= nbNewFields
                        || position - positionPerBackend[0] != 1) {
                    throw new IllegalStateException("unexpected");
                }
                ef.getAttributes().stream().map(Map.Entry::getValue).forEach(pos -> {
                    if (pos == null || pos < 0 || pos >= nbNewFields || pos - positionPerBackend[0] < 0
                            || pos - positionPerBackend[0] > 1) {
                        throw new IllegalStateException("unexpected");
                    }
                });
                positionPerBackend[0] = position;
                int nbAttributes = ef.getAttributes().size();
                int nbProtectedAttributes = ef.getProtectedFields().values().stream().flatMap(List::stream)
                        .map(ExpectedProtectedField::getAttributes).map(List::size).reduce(0, Integer::sum);
                if (nbProtectedAttributes != nbAttributes) {
                    LOGGER.trace("{} protected attribute(s) for {} clear attribute(s)", nbProtectedAttributes,
                            nbAttributes);
                    if (nbProtectedAttributes != nbAttributes * ef.getProtectedFields().size()) {
                        throw new IllegalStateException("unexpected");
                    }
                }
            }
        });
        return newFields;
    }

    private String toOutputName(String attributeName) {
        attributeName = attributeName.toLowerCase();
        int idx = attributeName.lastIndexOf('/');
        if (idx != -1) {
            attributeName = attributeName.substring(idx + 1);
        } else {
            // search for alias ("[ as] alias")
            idx = attributeName.lastIndexOf(" as ");
            if (idx != -1) {
                idx = idx + " as ".length();
            } else {
                idx = attributeName.lastIndexOf(" ");
                if (idx != -1) {
                    idx = idx + " ".length();
                }
            }
            if (idx != -1) {
                if (attributeName.substring(idx).chars().anyMatch(c -> {
                    return !(Character.isLetterOrDigit(c) || c == '_' || c == '$');
                }) || isKeyword(attributeName.substring(idx))) {
                    idx = -1;
                } else {
                    attributeName = attributeName.substring(idx);
                }
            }
            if (idx == -1) {
                idx = attributeName.indexOf('(');
                if (idx != -1) {
                    if (attributeName.substring(0, idx).trim().length() == 0) {
                        // sub expression
                        if (attributeName.substring(idx + 1).trim().startsWith("select")) {
                            // sub select
                            attributeName = attributeName.substring(idx + 1).trim().substring("select".length()).trim();
                            // extract first select item
                            idx = attributeName.indexOf(' ');
                            attributeName = attributeName.substring(0, idx);
                            attributeName = toOutputName(attributeName);
                        }
                    } else {
                        // function
                        attributeName = attributeName.substring(0, idx);
                        idx = attributeName.lastIndexOf('.');
                        if (idx != -1) {
                            // function prefixed with schema name
                            attributeName = attributeName.substring(idx + 1);
                        }
                    }
                } else {
                    // column
                    // remove double quote if any
                    attributeName = StringUtilities.unquote(attributeName);
                    idx = attributeName.lastIndexOf('.');
                    if (idx != -1) {
                        // column prefixed with table name (or table alias)
                        attributeName = attributeName.substring(idx + 1);
                    }
                    idx = attributeName.lastIndexOf('[');
                    if (idx != -1) {
                        // array index
                        attributeName = attributeName.substring(0, idx);
                    }
                    idx = attributeName.lastIndexOf("::");
                    if (idx != -1) {
                        // :: index
                        attributeName = attributeName.substring(0, idx);
                    }
                }
            }
        }
        return attributeName.trim();
    }

    private boolean isKeyword(String expression) {
        CCJSqlParser parser = new CCJSqlParser(new StringReader(expression));
        Token token = parser.getNextToken();
        return token != null && token.kind >= CCJSqlParserConstants.K_AS && token.kind <= CCJSqlParserConstants.K_SHOW
                && token.kind != CCJSqlParserConstants.K_WHEN && token.kind != CCJSqlParserConstants.K_ROWS;
    }

    private Map<Integer, List<Integer>> processRowJoinFields(ChannelHandlerContext ctx,
            SortedMap<Integer, List<PgsqlRowDescriptionMessage.Field>> allFields, List<ExpectedField> expectedFields) {
        Map<Integer, List<Integer>> joinFieldIndexes = null;
        String dataTechnicalId = getDataTechnicalId(ctx);
        Pattern dataTechnicalIdPattern = dataTechnicalId == null ? null : Pattern.compile(escapeRegex(dataTechnicalId));
        if (allFields.values().stream().anyMatch(l -> l.size() > 1) || (dataTechnicalIdPattern != null
                && expectedFields.stream().map(ExpectedField::getAttributes).flatMap(List::stream)
                        .map(Map.Entry::getKey).allMatch(an -> dataTechnicalIdPattern.matcher(an).matches()))) {
            Map<Integer, List<Map.Entry<String, Integer>>> attributesPerBackend = expectedFields != null
                    ? expectedFields.stream().map(ExpectedField::getProtectedFields).map(Map::values)
                            .flatMap(Collection::stream).flatMap(List::stream).collect(
                                    Collectors.toMap(ExpectedProtectedField::getBackend,
                                            ExpectedProtectedField::getAttributes, (a, b) -> Stream
                                                    .concat(a.stream(), b.stream()).collect(Collectors.toList())))
                    : null;
            if (attributesPerBackend != null && attributesPerBackend.values().stream().flatMap(List::stream)
                    .map(Map.Entry::getKey).anyMatch(an -> an.contains("geometry_columns/"))) {
                List<String> suffixes = Stream.of("f_table_schema", "f_table_name", "f_geometry_column")
                        .map(s -> "geometry_columns/" + s).collect(Collectors.toList());
                Map<Integer, List<Integer>> indexesPerBackend = attributesPerBackend.entrySet().stream()
                        .collect(
                                Collectors.toMap(Map.Entry::getKey,
                                        e -> suffixes
                                                .stream().map(
                                                        suffix -> IntStream.range(0, e.getValue().size())
                                                                .filter(i -> e.getValue().get(i).getKey()
                                                                        .endsWith(suffix))
                                                                .findFirst().orElse(-1))
                                                .collect(Collectors.toList())));
                if (indexesPerBackend.values().stream().allMatch(l -> l.stream().anyMatch(idx -> idx != -1))) {
                    joinFieldIndexes = indexesPerBackend.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey,
                                    e -> e.getValue().stream().filter(idx -> idx != -1)
                                            .map(idx -> attributesPerBackend.get(e.getKey()).get(idx))
                                            .map(Map.Entry::getValue).collect(Collectors.toList())));
                }
            } else {
                PgsqlRowDescriptionMessage.Field joinField = allFields.values().stream().flatMap(List::stream)
                        .filter(f -> allFields.values().stream().allMatch(l -> l.contains(f))).distinct()
                        .filter(f -> f.getTableOID() != 0)
                        .sorted(Comparator.comparing(PgsqlRowDescriptionMessage.Field::getColumnNumber)).findFirst()
                        .orElse(null);
                if (joinField != null) {
                    joinFieldIndexes = allFields.entrySet().stream().sorted(Map.Entry.comparingByKey())
                            .collect(Collectors.toMap(Map.Entry::getKey,
                                    e -> Collections.singletonList(e.getValue().indexOf(joinField))));
                }
            }
        }
        return joinFieldIndexes;
    }

    @Override
    public MessageTransferMode<List<ByteBuf>, Void> processDataRowResponse(ChannelHandlerContext ctx,
            List<ByteBuf> values) throws IOException {
        LOGGER.trace("Data row: {}", values);
        TransferMode transferMode;
        List<ByteBuf> newValues = null;
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        SortedMap<Integer, List<ByteBuf>> allValues = session.newQueryResponse(QueryResponseType.DATA_ROW, backend,
                numberOfBackends, values, session.getJoinFieldIndexes());
        if (allValues == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            // All backends have replied. Merge responses
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            if (nextQueryResponseToIgnore == null || nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY) {
                transferMode = TransferMode.FORWARD;
                if (numberOfBackends == 1) {
                    newValues = values;
                } else {
                    newValues = allValues.values().stream().flatMap(List::stream).collect(Collectors.toList());
                }
                if (session.getCurrentCommandOperation() == Operation.READ) {
                    if (FORCE_SQL_PROCESSING || session.isResultProcessingEnabled()) {
                        newValues = processDataRow(ctx, allValues, involvedBackends);
                        if (LOGGER.isTraceEnabled()) {
                            if (numberOfBackends == 1 && !newValues.equals(values)) {
                                LOGGER.trace("values were modified");
                            }
                        }
                    }
                }
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("{} new value(s) replace {} old value(s) from {} backend(s)", newValues.size(),
                            allValues.values().stream().flatMap(List::stream).count(), numberOfBackends);
                }
                if (newValues == values || newValues.stream().allMatch(nv -> values.stream().anyMatch(v -> v == nv))) {
                    LOGGER.trace("new values from one backend");
                    // release the initial buffers that has been retained by
                    // session.addQueryResponse (avoid memory leaks)
                    newValues.stream().filter(nv -> nv != null).forEach(nv -> {
                        if (nv.release() && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("value {} deallocated", nv);
                        }
                    });
                } else if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("at list one value is new or is from another backend");
                    newValues.stream().filter(nv -> values.stream().noneMatch(v -> v == nv)).forEach(nv -> {
                        LOGGER.trace("value {} is new or is from another backend", nv);
                    });
                }
                // release buffers of unused values
                List<ByteBuf> nvs = newValues;
                allValues.values().stream().flatMap(List::stream).filter(v -> v != null).forEach(v -> {
                    if (nvs.stream().noneMatch(nv -> nv == v)) {
                        if (v.release() && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("value {} deallocated", v);
                        }
                    }
                });
            } else if (nextQueryResponseToIgnore == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                transferMode = TransferMode.FORGET;
            } else {
                throw new IllegalStateException(
                        String.format("Unexpected %s response (%s or null was expected)", nextQueryResponseToIgnore,
                                QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR));
            }
        }
        MessageTransferMode<List<ByteBuf>, Void> mode = new MessageTransferMode<List<ByteBuf>, Void>(transferMode,
                newValues);
        LOGGER.trace("Data row processed: new values={}, transfer mode={}", mode.getNewContent(),
                mode.getTransferMode());
        return mode;
    }

    private List<ByteBuf> processDataRow(ChannelHandlerContext ctx, SortedMap<Integer, List<ByteBuf>> allValues,
            List<Integer> involvedBackends) {
        List<ByteBuf> newValues;
        SQLSession session = getSession(ctx);
        if (session.isUnprotectingDataEnabled() && ((session.getExpectedFields() != null
                && session.getBackendRowDescriptions() != null && session.getRowDescription() != null)
                || (session.getCurrentDescribeStepStatus() != null
                        && session.getCurrentDescribeStepStatus().getExpectedFields() != null
                        && session.getCurrentDescribeStepStatus().getBackendRowDescriptions() != null
                        && session.getCurrentDescribeStepStatus().getRowDescription() != null))) {
            // Extract data operations
            int maxBackend = allValues.keySet().stream().max(Comparator.naturalOrder()).get();
            Map<Integer, List<Field>> backendFields = session.getBackendRowDescriptions();
            if (backendFields == null && session.getCurrentDescribeStepStatus() != null) {
                backendFields = session.getCurrentDescribeStepStatus().getBackendRowDescriptions();
            }
            if (backendFields == null) {
                throw new IllegalStateException("unexpected");
            }
            List<ExpectedField> expectedFields = session.getExpectedFields();
            if (expectedFields == null && session.getCurrentDescribeStepStatus() != null) {
                expectedFields = session.getCurrentDescribeStepStatus().getExpectedFields();
            }
            if (expectedFields == null) {
                throw new IllegalStateException("unexpected");
            }
            List<CString> clearDataIds = expectedFields.stream().filter(ep -> ep.getPosition() != -1)
                    .map(ExpectedField::getAttributes).flatMap(List::stream).map(Map.Entry::getKey)
                    .map(CString::valueOf).collect(Collectors.toList());
            SQLDatabaseSchema databaseSchema = getDatabaseSchema(ctx);
            Integer srid = clearDataIds.stream().map(CString::toString).filter(this::isFullyQualifiedDataId)
                    .map(did -> did.substring(0, did.lastIndexOf('/'))).map(databaseSchema::getDatasetSrid)
                    .filter(s -> s != null).map(Integer::valueOf).findFirst().orElse(null);
            List<PgsqlRowDescriptionMessage.Field> fields = session.getRowDescription();
            if (fields == null && session.getCurrentDescribeStepStatus() != null) {
                fields = session.getCurrentDescribeStepStatus().getRowDescription();
            }
            if (fields == null) {
                throw new IllegalStateException("unexpected");
            }
            boolean[] sridAddedFlags = new boolean[fields.size()];
            Arrays.fill(sridAddedFlags, false);
            List<InboundDataOperation> inboundDataOperations = new ArrayList<>(maxBackend + 1);
            for (int backend = 0; backend < maxBackend + 1; backend++) {
                if (!involvedBackends.contains(backend)) {
                    continue;
                }
                InboundDataOperation inboundDataOperation = new InboundDataOperation();
                inboundDataOperation.setInvolvedCSP(backend);
                inboundDataOperation.setOperation(Operation.READ);
                inboundDataOperation.setClearDataIds(clearDataIds);
                inboundDataOperation.setPromise(session.getPromise());
                int csp = backend;
                List<CString> dataIds = expectedFields.stream().filter(ef -> ef.getPosition() != -1)
                        .map(ExpectedField::getProtectedFields).map(Map::entrySet).flatMap(Set::stream)
                        .filter(e -> e.getKey() == csp).map(Map.Entry::getValue).flatMap(List::stream)
                        .flatMap(epf -> epf.getAttributes().stream().map(Map.Entry::getKey)).map(CString::valueOf)
                        .collect(Collectors.toList());
                inboundDataOperation.setDataIds(dataIds);
                List<CString> dataValues = new ArrayList<>(dataIds.size());
                List<ByteBuf> values = allValues.get(backend);
                for (ExpectedField expectedField : expectedFields) {
                    List<ExpectedProtectedField> backendProtectedFields = expectedField
                            .getBackendProtectedFields(backend);
                    if (backendProtectedFields != null) {
                        for (ExpectedProtectedField protectedField : backendProtectedFields) {
                            for (Map.Entry<String, Integer> entry2 : protectedField.getAttributes()) {
                                int position = entry2.getValue();
                                PgsqlRowDescriptionMessage.Field field = backendFields.get(backend).get(position);
                                Types backendTypes = getTypes(ctx, backend);
                                Type type = backendTypes.getType(field.getTypeOID());
                                CString dataValue = convertToText(type, field.getTypeModifier(), field.getFormat(),
                                        values.get(position));
                                if (srid != null) {
                                    if (type == null) {
                                        for (Type typeWithUnfixedOid : backendTypes.getTypesWithUnfixedOid()) {
                                            try {
                                                TypeParser.parse(typeWithUnfixedOid, field.getTypeModifier(),
                                                        dataValue);
                                                type = typeWithUnfixedOid;
                                                backendTypes.setTypeOid(type.getName(), field.getTypeOID());
                                                break;
                                            } catch (IllegalArgumentException e) {
                                                // not critical
                                            }
                                        }
                                    }
                                    if (type != null) {
                                        boolean sridModified = false;
                                        Object object = TypeParser.parse(type, field.getTypeModifier(), dataValue);
                                        if (object instanceof Geometry) {
                                            Geometry geometry = (Geometry) object;
                                            if (geometry.getSrid() == 0) {
                                                geometry.setSrid(srid);
                                                dataValue = TypeWriter.toCString(type, geometry);
                                                sridModified = true;
                                            }
                                        } else if (object instanceof PGboxbase) {
                                            PGboxbase box = (PGboxbase) object;
                                            if (box.getLLB().getSrid() == 0 || box.getURT().getSrid() == 0) {
                                                if (box.getLLB().getSrid() == 0) {
                                                    box.getLLB().setSrid(srid);
                                                }
                                                if (box.getURT().getSrid() == 0) {
                                                    box.getURT().setSrid(srid);
                                                }
                                                dataValue = TypeWriter.toCString(type, box);
                                                sridModified = true;
                                            }
                                        }
                                        if (sridModified) {
                                            sridAddedFlags[expectedField.getPosition()] = true;
                                        }
                                    }
                                }
                                dataValues.add(dataValue);
                            }
                        }
                    }
                }
                inboundDataOperation.setDataValues(Collections.singletonList(dataValues));
                inboundDataOperations.add(inboundDataOperation);
            }
            // Process data operation
            InboundDataOperation newInboundDataOperation = newInboundDataOperation(ctx, inboundDataOperations);
            if (FORCE_SQL_PROCESSING || newInboundDataOperation.isModified()) {
                if (newInboundDataOperation.getDataValues().isEmpty()) {
                    throw new IllegalStateException("unexpected");
                }
                List<CString> dataValues = newInboundDataOperation.getDataValues().get(0);
                newValues = Stream.generate((Supplier<ByteBuf>) () -> null).limit(fields.size())
                        .collect(Collectors.toList());
                int index = 0;
                for (int i = 0; i < expectedFields.size(); i++) {
                    ExpectedField expectedField = expectedFields.get(i);
                    if (expectedField.getPosition() == -1) {
                        continue;
                    }
                    for (int j = 0; j < expectedField.getAttributes().size(); j++) {
                        Map.Entry<String, Integer> entry = expectedField.getAttributes().get(j);
                        int position = entry.getValue();
                        if (newValues.get(position) == null) {
                            PgsqlRowDescriptionMessage.Field field = fields.get(position);
                            CString dataValue = dataValues.get(index);
                            Types types = getTypes(ctx, -1);
                            Type type = types.getType(field.getTypeOID());
                            if (sridAddedFlags[position]) {
                                if (type == null) {
                                    for (Type typeWithUnfixedOid : types.getTypesWithUnfixedOid()) {
                                        try {
                                            TypeParser.parse(typeWithUnfixedOid, field.getTypeModifier(), dataValue);
                                            type = typeWithUnfixedOid;
                                            types.setTypeOid(type.getName(), field.getTypeOID());
                                            break;
                                        } catch (IllegalArgumentException e) {
                                            // not critical
                                        }
                                    }
                                }
                                if (type != null) {
                                    Object object = TypeParser.parse(type, field.getTypeModifier(), dataValue);
                                    if (object instanceof Geometry) {
                                        Geometry geometry = (Geometry) object;
                                        if (geometry.getSrid() != 0) {
                                            geometry.setSrid(0);
                                            dataValue = TypeWriter.toCString(type, geometry);
                                        }
                                    } else if (object instanceof PGboxbase) {
                                        PGboxbase box = (PGboxbase) object;
                                        if (box.getLLB().getSrid() != 0 || box.getURT().getSrid() != 0) {
                                            if (box.getLLB().getSrid() != 0) {
                                                box.getLLB().setSrid(0);
                                            }
                                            if (box.getURT().getSrid() != 0) {
                                                box.getURT().setSrid(0);
                                            }
                                            dataValue = TypeWriter.toCString(type, box);
                                        }
                                    }
                                }
                            }
                            ByteBuf byteBuf = convertToByteBuf(type, field.getTypeModifier(), field.getFormat(),
                                    dataValue);
                            newValues.set(position, byteBuf);
                        }
                        index++;
                    }
                }
                if (!newInboundDataOperation.isModified()) {
                    List<ByteBuf> values = allValues.get(involvedBackends.get(0));
                    values.stream().filter(v -> v != null).forEach(v -> v.resetReaderIndex());
                    if (!newValues.equals(values)) {
                        throw new IllegalStateException("unexpected");
                    }
                }
            } else {
                newValues = allValues.get(involvedBackends.get(0));
            }
        } else {
            if (involvedBackends.size() > 1) {
                // retain values according to the involved backends
                // for each involved backend
                newValues = involvedBackends.stream()
                        // get backend values
                        .flatMap(backend -> allValues.get(backend).stream())
                        // save values as list
                        .collect(Collectors.toList());
            } else {
                newValues = allValues.get(involvedBackends.get(0));
            }
        }
        return newValues;
    }

    private CString convertToText(Type type, int typeModifier, short format, ByteBuf value) {
        CString cs;
        if (format == 0) {
            // Text format
            cs = value != null ? CString.valueOf(value, value.capacity()) : null;
            // Special case for geometry type: remove '\x'
            if (type == Type.BYTEA && cs.startsWith("\\x")) {
                cs = cs.substring("\\x".length());
            }
        } else {
            // Binary format
            Object object = TypeParser.parse(type, typeModifier, value);
            cs = TypeWriter.toCString(type, object);
        }
        return cs;
    }

    private ByteBuf convertToByteBuf(Type type, int typeModifier, short format, CString cs) {
        ByteBuf value;
        if (format == 0) {
            // Text format
            if (cs != null) {
                // Special case for geometry type: prepend with '\x'
                if (type == Type.BYTEA && !cs.startsWith("\\x")) {
                    cs = CString.valueOf("\\x").append(cs);
                }
                value = cs.getByteBuf(cs.length());
            } else {
                value = null;
            }
        } else {
            // Binary format
            Object object = TypeParser.parse(type, typeModifier, cs);
            value = TypeWriter.getBytes(type, object);
        }
        return value;
    }

    @Override
    public MessageTransferMode<Void, Void> processNoDataResponse(ChannelHandlerContext ctx) {
        LOGGER.debug("No data");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        // note use Boolean instead of Void because insertion of null is not
        // permitted
        SortedMap<Integer, Boolean> all = session.newQueryResponse(QueryResponseType.NO_DATA, backend, numberOfBackends,
                Boolean.TRUE);
        if (all == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            // All backends have replied. Merge responses
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            if (nextQueryResponseToIgnore == null) {
                transferMode = TransferMode.FORWARD;
            } else if (nextQueryResponseToIgnore == QueryResponseType.ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA) {
                transferMode = TransferMode.FORGET;
                session.removeFirstQueryResponseToIgnore();
            } else {
                throw new IllegalStateException(String.format("Unexpected %s response (%s or null was expected)",
                        nextQueryResponseToIgnore, QueryResponseType.ROW_DESCRIPTION_AND_DATA_ROW_OR_NO_DATA));
            }
        }
        MessageTransferMode<Void, Void> mode = new MessageTransferMode<>(transferMode, null);
        LOGGER.debug("No data processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<CString, Void> processCommandCompleteResult(ChannelHandlerContext ctx, CString tag)
            throws IOException {
        LOGGER.debug("Command complete: {}", tag);
        TransferMode transferMode;
        CString newTag = null;
        Map<Byte, CString> errorDetails = null;
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        SortedMap<Integer, CString> allTags = session.newQueryResponse(QueryResponseType.COMMAND_COMPLETE, backend,
                numberOfBackends, tag);
        if (allTags == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            if (CHECK_BUFFER_REFERENCE_COUNT) {
                allTags.values().forEach(tg -> {
                    int refCnt = tg.refCnt();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("in tag {} ({}) reference count: {}", tg, System.identityHashCode(tg), refCnt);
                    }
                    if (refCnt == 0) {
                        throw new IllegalStateException(String
                                .format("Unexpected reference count (0) for tag '%s' (expected greater than 0)", tg));
                    }
                });
            }
            // All backends have replied. Merge responses
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            if (nextQueryResponseToIgnore == null || nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY
                    || nextQueryResponseToIgnore == QueryResponseType.PARSE_COMPLETE) {
                transferMode = TransferMode.FORWARD;
                if (numberOfBackends == 1) {
                    newTag = tag;
                } else {
                    if (allTags.values().stream().distinct().count() > 1) {
                        Map<String, List<String[]>> tagsByCommand = allTags.values().stream().map(CString::toString)
                                .map(s -> s.split(" ")).collect(Collectors.groupingBy(tk -> tk[0]));
                        if (tagsByCommand.size() == 1) {
                            String command = tagsByCommand.keySet().stream().findAny().get();
                            Stream<String> commands = Stream.of("INSERT", "UPDATE", "DELETE", "SELECT", "FETCH", "MOVE",
                                    "COPY");
                            if (commands.anyMatch(c -> c.equalsIgnoreCase(command))) {
                                newTag = allTags.values().stream().sorted().findFirst().get();
                            }
                        }
                        if (newTag == null) {
                            transferMode = TransferMode.ERROR;
                            errorDetails = new LinkedHashMap<>();
                            errorDetails.put((byte) 'S', CString.valueOf("FATAL"));
                            errorDetails.put((byte) 'M',
                                    CString.valueOf("Unexpected different tags from the backends"));
                        }
                    } else {
                        newTag = tag;
                    }
                }
                session.resetCurrentCommand();
                // Release buffers if necessary
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("{} new tag replace {} old tag(s) from {} backend(s)", 1,
                            allTags.values().stream().count(), numberOfBackends);
                }
                if (newTag == tag) {
                    LOGGER.trace("new tag from one backend");
                    // release the initial buffers that has been retained by
                    // session.addQueryResponse (avoid memory leaks)
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("releasing {} ({}), refcount={}", newTag, System.identityHashCode(newTag),
                                ((ReferenceCounted) newTag).refCnt());
                    }
                    if (newTag.release() && LOGGER.isTraceEnabled()) {
                        LOGGER.trace("tag {} deallocated", newTag);
                    }
                } else if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("tag {} is new or is from another backend", newTag);
                }
                // release buffers of unused tags
                CString ntg = newTag;
                allTags.values().stream().forEach(tg -> {
                    if (ntg != tg) {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("releasing {} ({}), refcount={}", tg, System.identityHashCode(tg),
                                    ((ReferenceCounted) tg).refCnt());
                        }
                        if (tg.release() && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("tag {} deallocated", tg);
                        }
                    }
                });
                if (CHECK_BUFFER_REFERENCE_COUNT) {
                    allTags.values().forEach(tg -> {
                        int refCnt = tg.refCnt();
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("out tag {} ({}) reference count: {}", tg, System.identityHashCode(tg),
                                    refCnt);
                        }
                        if (tg == ntg) {
                            if (refCnt == 0) {
                                throw new IllegalStateException(String.format(
                                        "Unexpected reference count (0) for tag '%s' (expected greater than 0)", tg));
                            }
                        } else if (tg == tag) {
                            if (refCnt == 0) {
                                throw new IllegalStateException(String.format(
                                        "Unexpected reference count (0) for tag '%s' (expected greater than 0)", tg));
                            }
                        } else {
                            if (refCnt != 0) {
                                throw new IllegalStateException(String.format(
                                        "Unexpected reference count (%d) for tag '%s' (expected 0)", refCnt, tg));
                            }
                        }
                    });
                }
            } else if (nextQueryResponseToIgnore == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                transferMode = TransferMode.FORGET;
                session.removeFirstQueryResponseToIgnore();
            } else {
                throw new IllegalStateException(
                        String.format("Unexpected %s response (%s or null was expected)", nextQueryResponseToIgnore,
                                QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR));
            }
        }
        MessageTransferMode<CString, Void> mode = new MessageTransferMode<>(transferMode, newTag);
        LOGGER.debug("Command complete processed: new tag={}, transfer mode={}", mode.getNewContent(),
                mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Void, Void> processEmptyQueryResponse(ChannelHandlerContext ctx) throws IOException {
        LOGGER.debug("Empty query");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        // Note: use Boolean instead of Void because insertion of null is not
        // permitted
        SortedMap<Integer, Boolean> all = session.newQueryResponse(QueryResponseType.EMPTY_QUERY, backend,
                numberOfBackends, Boolean.TRUE);
        if (all == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            // All backends have replied. Merge responses
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            if (nextQueryResponseToIgnore == null || nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY) {
                transferMode = TransferMode.FORWARD;
                session.resetCurrentCommand();
            } else if (nextQueryResponseToIgnore == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                transferMode = TransferMode.FORGET;
                session.removeFirstQueryResponseToIgnore();
            } else {
                throw new IllegalStateException(
                        String.format("Unexpected %s response (%s or null was expected)", nextQueryResponseToIgnore,
                                QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR));
            }
        }
        MessageTransferMode<Void, Void> mode = new MessageTransferMode<>(transferMode, null);
        LOGGER.debug("Empty query processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Void, Void> processPortalSuspendedResponse(ChannelHandlerContext ctx)
            throws IOException {
        LOGGER.debug("Portal suspended");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        // Use Boolean instead of Void because insertion of null is not
        // permitted
        SortedMap<Integer, Boolean> all = session.newQueryResponse(QueryResponseType.PORTAL_SUSPENDED, backend,
                numberOfBackends, Boolean.TRUE);
        if (all == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            // All backends have replied. Merge responses
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            if (nextQueryResponseToIgnore == null || nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY) {
                transferMode = TransferMode.FORWARD;
            } else if (nextQueryResponseToIgnore == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                transferMode = TransferMode.FORGET;
                session.removeFirstQueryResponseToIgnore();
            } else {
                throw new IllegalStateException(
                        String.format("Unexpected %s response (%s or null was expected)", nextQueryResponseToIgnore,
                                QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR));
            }
        }
        MessageTransferMode<Void, Void> mode = new MessageTransferMode<>(transferMode, null);
        LOGGER.debug("Portal suspended processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Map<Byte, CString>, Void> processErrorResult(ChannelHandlerContext ctx,
            Map<Byte, CString> fields) throws IOException {
        LOGGER.debug("Error: {}", fields);
        TransferMode transferMode;
        Map<Byte, CString> newFields = null;
        // Save current error
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getQueryInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        SortedMap<Integer, Map<Byte, CString>> allFields = session.newQueryResponse(QueryResponseType.ERROR, backend,
                numberOfBackends, fields);
        if (allFields == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            // All backends have replied. Merge responses
            session.setTransactionErrorDetails(fields);
            // Skip any expected response before expected ready for query
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            while (nextQueryResponseToIgnore != null
                    && nextQueryResponseToIgnore != QueryResponseType.READY_FOR_QUERY) {
                session.removeFirstQueryResponseToIgnore();
                nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            }
            if (session.getQueryResponsesToIgnore().isEmpty()) {
                transferMode = TransferMode.FORWARD;
                newFields = fields;
                session.resetCurrentCommand();
                // Release buffers if necessary
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("{} new field(s) replace {} old field(s) from {} backend(s)", newFields.size(),
                            allFields.values().stream().map(Map::entrySet).flatMap(Collection::stream).count(),
                            numberOfBackends);
                }
                if (newFields == fields || newFields.entrySet().stream().allMatch(ne -> fields.entrySet().stream()
                        .anyMatch(e -> e.getKey() == ne.getKey() && e.getValue() == ne.getValue()))) {
                    LOGGER.trace("new fields from one backend");
                    // release the initial buffers that has been retained by
                    // session.addQueryResponse (avoid memory leaks)
                    newFields.values().stream().forEach(nf -> {
                        if (nf.release() && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("field {} deallocated", nf);
                        }
                    });
                } else if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("at list one field is new or is from another backend");
                    newFields.entrySet().stream()
                            .filter(ne -> fields.entrySet().stream()
                                    .noneMatch(e -> e.getKey() == ne.getKey() && e.getValue() == ne.getValue()))
                            .forEach(ne -> {
                                LOGGER.trace("field {}:{} is new or is from another backend", ne.getKey(),
                                        ne.getValue());
                            });
                }
                // release buffers of unused fields
                Map<Byte, CString> nfs = newFields;
                allFields.values().stream().map(Map::values).flatMap(Collection::stream).forEach(f -> {
                    if (nfs.values().stream().noneMatch(nf -> nf == f)) {
                        if (f.release() && LOGGER.isTraceEnabled()) {
                            LOGGER.trace("field {} deallocated", f);
                        }
                    }
                });
            } else {
                transferMode = TransferMode.FORGET;
            }
            session.responsesReceived();
        }
        MessageTransferMode<Map<Byte, CString>, Void> mode = new MessageTransferMode<>(transferMode, newFields);
        LOGGER.debug("Error processed: new fields={}, transfer mode={}", mode.getNewContent(), mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Void, Void> processCloseCompleteResponse(ChannelHandlerContext ctx) {
        LOGGER.debug("Close complete");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getCommandInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        // Note: use Boolean instead of Void because insertion of null is not
        // permitted
        SortedMap<Integer, Boolean> all = session.newQueryResponse(QueryResponseType.CLOSE_COMPLETE, backend,
                numberOfBackends, Boolean.TRUE);
        if (all == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            // All backends have replied. Merge responses
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            if (nextQueryResponseToIgnore == null) {
                transferMode = TransferMode.FORWARD;
            } else if (nextQueryResponseToIgnore == QueryResponseType.CLOSE_COMPLETE) {
                transferMode = TransferMode.FORGET;
                session.removeFirstQueryResponseToIgnore();
            } else {
                throw new IllegalStateException(String.format("Unexpected %s response (%s or null was expected)",
                        nextQueryResponseToIgnore, QueryResponseType.CLOSE_COMPLETE));
            }
        }
        MessageTransferMode<Void, Void> mode = new MessageTransferMode<>(transferMode, null);
        LOGGER.debug("Close complete processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Byte, Void> processReadyForQueryResponse(ChannelHandlerContext ctx,
            Byte transactionStatus) throws IOException {
        LOGGER.debug("Ready for query: {}", (char) transactionStatus.byteValue());
        TransferMode transferMode;
        Byte newTransactionStatus = null;
        SQLSession session = getSession(ctx);
        // Ensure that all backends have replied
        int backend = getBackend(ctx);
        List<Integer> involvedBackends = session.getQueryInvolvedBackends();
        int numberOfBackends = involvedBackends.size();
        // Retrieve response if all backends have replied
        SortedMap<Integer, Byte> allTransactionStatus = session.newQueryResponse(QueryResponseType.READY_FOR_QUERY,
                backend, numberOfBackends, transactionStatus);
        if (allTransactionStatus == null) {
            // Missing backend replies
            transferMode = TransferMode.FORGET;
        } else {
            // All backends have replied. Merge responses
            QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
            if (nextQueryResponseToIgnore == null) {
                transferMode = TransferMode.FORWARD;
                if (numberOfBackends == 1) {
                    newTransactionStatus = transactionStatus;
                } else {
                    // find any transaction error
                    newTransactionStatus = allTransactionStatus.values().stream().distinct()
                            .filter(ts -> ts == (byte) 'E').findAny().orElse(transactionStatus);
                }
                // Save current transaction status
                session.setTransactionStatus(transactionStatus);
                if (transactionStatus != (byte) 'E') {
                    // Reset current error
                    session.setTransactionErrorDetails(null);
                }
                session.resetCurrentQuery();
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("{} new transaction status replace {} old transaction status(s) from {} backend(s)", 1,
                            allTransactionStatus.values().stream().count(), numberOfBackends);
                }
                if (newTransactionStatus == transactionStatus) {
                    LOGGER.trace("new transaction status from one backend");
                } else if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("transaction status {} is new or is from another backend", newTransactionStatus);
                }
            } else if (nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY) {
                transferMode = TransferMode.FORGET;
                session.removeFirstQueryResponseToIgnore();
                session.resetCurrentQuery();
            } else {
                throw new IllegalStateException(String.format("Unexpected %s response (%s or null was expected)",
                        nextQueryResponseToIgnore, QueryResponseType.READY_FOR_QUERY));
            }
            session.responsesReceived();
        }
        MessageTransferMode<Byte, Void> mode = new MessageTransferMode<>(transferMode, newTransactionStatus);
        LOGGER.debug("Ready for query processed: new transaction status={}, transfer mode={}",
                newTransactionStatus == null ? null : (char) newTransactionStatus.byteValue(), mode.getTransferMode());
        return mode;
    }

    private PgsqlSession getPgsqlSession(ChannelHandlerContext ctx) {
        PgsqlSession pgsqlSession = (PgsqlSession) ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        return pgsqlSession;
    }

    private SQLSession getSession(ChannelHandlerContext ctx) {
        PgsqlSession pgsqlSession = getPgsqlSession(ctx);
        return pgsqlSession.getSqlSession();
    }

    private SQLDatabaseSchema getDatabaseSchema(ChannelHandlerContext ctx) {
        Map<String, Object> customData = ctx.channel().attr(PgsqlConstants.CUSTOM_DATA_KEY).get();
        SQLDatabaseSchema sqlDatabaseSchema = (SQLDatabaseSchema) customData.get(SQL_DATABASE_SCHEMA);
        if (sqlDatabaseSchema == null) {
            sqlDatabaseSchema = new SQLDatabaseSchema();
            customData.put(SQL_DATABASE_SCHEMA, sqlDatabaseSchema);
        }
        return sqlDatabaseSchema;
    }

    private Types getTypes(ChannelHandlerContext ctx, int backend) {
        SQLDatabaseSchema sqlDatabaseSchema = getDatabaseSchema(ctx);
        Types types = backend != -1 ? sqlDatabaseSchema.getBackendTypes(backend) : sqlDatabaseSchema.getTypes();
        return types;
    }

    private Mode getProcessingMode(ChannelHandlerContext ctx, boolean wholeDataset, Operation operation) {
        Configuration configuration = ctx.channel().attr(PgsqlConstants.CONFIGURATION_KEY).get();
        return configuration.getProcessingMode(wholeDataset, operation);
    }

    private ProtocolService getProtocolService(ChannelHandlerContext ctx) {
        Configuration configuration = ctx.channel().attr(PgsqlConstants.CONFIGURATION_KEY).get();
        return configuration.getProtocolService();
    }

    private String getDataTechnicalId(ChannelHandlerContext ctx) {
        PgsqlConfiguration configuration = (PgsqlConfiguration) ctx.channel().attr(PgsqlConstants.CONFIGURATION_KEY)
                .get();
        return configuration.getDataTechnicalId();
    }

    private Map<String, String> getGeometryObjectDefinition(ChannelHandlerContext ctx) {
        PgsqlConfiguration configuration = (PgsqlConfiguration) ctx.channel().attr(PgsqlConstants.CONFIGURATION_KEY)
                .get();
        return configuration.getGeometryObjectDefinition();
    }

    private String getDatabaseName(ChannelHandlerContext ctx) {
        SQLSession session = getSession(ctx);
        return session.getDatabaseName();
    }

    private List<String> getBackendDatabaseNames(ChannelHandlerContext ctx) {
        PgsqlConfiguration configuration = (PgsqlConfiguration) ctx.channel().attr(PgsqlConstants.CONFIGURATION_KEY)
                .get();
        return configuration.getBackendDatabaseNames();
    }

    private int getNumberOfBackends(ChannelHandlerContext ctx) {
        PgsqlSession pgsqlSession = (PgsqlSession) ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        return pgsqlSession.getServerSideChannels().size();
    }

    private int getBackend(ChannelHandlerContext ctx) {
        Integer serverEndpointNumber = ctx.channel().attr(TCPConstants.SERVER_ENDPOINT_NUMBER_KEY).get();
        if (serverEndpointNumber == null) {
            throw new NullPointerException(TCPConstants.SERVER_ENDPOINT_NUMBER_KEY.name() + " is not set");
        }
        int numberOfBackends = getNumberOfBackends(ctx);
        if (serverEndpointNumber < 0 || serverEndpointNumber >= numberOfBackends) {
            throw new IndexOutOfBoundsException(String.format("invalid %s: value: %d, number of server endpoints: %d ",
                    TCPConstants.SERVER_ENDPOINT_NUMBER_KEY.name(), serverEndpointNumber, numberOfBackends));
        }
        return serverEndpointNumber;
    }

    private int getPreferredBackend(ChannelHandlerContext ctx) {
        Integer preferredServerEndpoint = ctx.channel().attr(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY).get();
        if (preferredServerEndpoint == null) {
            throw new NullPointerException(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY.name() + " is not set");
        }
        int numberOfBackends = getNumberOfBackends(ctx);
        if (preferredServerEndpoint < 0 || preferredServerEndpoint >= numberOfBackends) {
            throw new IndexOutOfBoundsException(String.format("invalid %s: value: %d, number of server endpoints: %d ",
                    TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY.name(), preferredServerEndpoint, numberOfBackends));
        }
        return preferredServerEndpoint;
    }

}
