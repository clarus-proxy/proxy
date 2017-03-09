package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConstants;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession.ExtendedQueryStatus;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession.QueryResponseType;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.MetadataOperation;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.ModuleOperation;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.StringUtilities;
import eu.clarussecure.proxy.spi.protection.DefaultPromise;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.TokenMgrError;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
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

    public static final String USER_KEY = "user";
    public static final String DATABASE_KEY = "database";

    private final static int AUTHENTICATION_CLEARTEXT_PASSWORD = 3;
    private final static int AUTHENTICATION_MD5_PASSWORD = 5;

    public static final String FUNCTION_METADATA = "CLARUS_METADATA";
    public static final String FUNCTION_PROTECTED = "CLARUS_PROTECTED";

    @Override
    public CString processUserAuthentication(ChannelHandlerContext ctx, Map<CString, CString> parameters)
            throws IOException {
        CString databaseName = parameters.get(CString.valueOf(DATABASE_KEY));
        SQLSession sqlSession = getSession(ctx);
        sqlSession.setDatabaseName((CString) databaseName.clone());
        CString userName = getProtocolService(ctx).newUserIdentification(parameters.get(CString.valueOf(USER_KEY)));
        // Add user to session.
        sqlSession.setUser(userName);
        return userName;
    }

    @Override
    public int processAuthenticationParameters(ChannelHandlerContext ctx, int authenticationType,
            ByteBuf authenticationParam) throws IOException {
        int newAuthenticationType = authenticationType;
        SQLSession sqlSession = getSession(ctx);

        // Add authentication parameters to session.
        // Add authentication type to session.
        sqlSession.setAuthenticationParam(authenticationParam);
        sqlSession.setAuthenticationType(authenticationType);

        // Case where authentication type is MD5, modify type to Cleartext.
        if (AUTHENTICATION_MD5_PASSWORD == authenticationType) {
            newAuthenticationType = AUTHENTICATION_CLEARTEXT_PASSWORD;
        }
        return newAuthenticationType;
    }

    @Override
    public CString processAuthentication(ChannelHandlerContext ctx, CString passwordClear)
            throws IOException, NoSuchAlgorithmException {
        SQLSession sqlSession = getSession(ctx);
        CString userName = sqlSession.getUser();
        CString password = passwordClear;

        // Handle case where MD5 password is needed.
        if (AUTHENTICATION_MD5_PASSWORD == sqlSession.getAuthenticationType()) {
            ByteBuf authenticationParam = sqlSession.getAuthenticationParam();
            if (authenticationParam != null) {
                // Retrieve salt from authentication parameters.
                byte[] salt = new byte[authenticationParam.readableBytes()];
                authenticationParam.readBytes(salt);

                // Create MD5 digester.
                MessageDigest digest = MessageDigest.getInstance("MD5");

                // Generate MD5 hash for password + user name.
                digest.update((password.toString() + userName.toString()).getBytes());
                String pwdUsrEnc = DatatypeConverter.printHexBinary(digest.digest()).toLowerCase();

                // Generate MD5 hash for hashed password + user name & salt.
                // Finally, add "md5" at the newly hashed password.
                digest.update(pwdUsrEnc.getBytes());
                digest.update(salt);
                String passwordEncrypted = "md5" + DatatypeConverter.printHexBinary(digest.digest()).toLowerCase();

                // MD5 password is equivalent to SQL concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
                password = CString.valueOf(passwordEncrypted);
            }
        }
        CString[] userCredentials = getProtocolService(ctx).userAuthentication(userName, password);
        return userCredentials[1];
    }

    @Override
    public QueriesTransferMode<SQLStatement, CommandResults> processStatement(ChannelHandlerContext ctx,
            SQLStatement sqlStatement) throws IOException {
        LOGGER.debug("SQL statement: {}", sqlStatement);
        TransferMode transferMode = TransferMode.FORWARD;
        List<Query> newQueries = Collections.singletonList(sqlStatement);
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
            case START_TRANSACTION: {
                toProcess = isSQLStatementToProcess(null);
                if (sqlStatement instanceof SimpleQuery && session.getTransactionStatus() != (byte) 'E') {
                    // transaction status is followed by tracking ready for query responses
                    // however, in case of simple query, starting a transaction may be part of a whole script in a single query
                    // in that case, we can't wait for ready for query response
                    // so we suppose starting a transaction is ok (provided that transaction status is not error)
                    session.setTransactionStatus((byte) 'T');
                }
                break;
            }
            case COMMIT:
            case ROLLBACK: {
                toProcess = isSQLStatementToProcess(null);
                if (sqlStatement instanceof SimpleQuery && session.getTransactionStatus() != (byte) 'E') {
                    // transaction status is followed by tracking ready for query responses
                    // however, in case of simple query, closing a transaction may be part of a whole script in a single query
                    // in that case, we can't wait for ready for query response
                    // so we suppose closing a transaction is ok (provided that transaction status is not error)
                    session.setTransactionStatus((byte) 'I');
                }
                if (session.isInDatasetCreation()) {
                    // Closing a transaction completes creation of a dataset (arbitrary decision)
                    session.setInDatasetCreation(false);
                }
                break;
            }
            case SELECT: {
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
                        // TODO orchestration mode. Meanwhile, same as AS_IT_IS mode
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
                Mode processingMode = getProcessingMode(ctx, true, Operation.CREATE);
                toProcess = isSQLStatementToProcess(processingMode);
                if (toProcess) {
                    if (processingMode == null) {
                        transferMode = TransferMode.ERROR;
                        error = CString.valueOf("Dataset creation not supported by this CLARUS proxy");
                    } else {
                        if (session.getTransactionStatus() == (byte) 'T') {
                            session.setInDatasetCreation(true);
                        }
                    }
                }
                break;
            }
            case ADD_GEOMETRY_COLUMN: {
                toProcess = true;
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
        session.setTransferMode(transferMode);
        if (transferMode == TransferMode.FORWARD) {
            if (sqlStatement instanceof SimpleQuery) {
                // Process simple query immediately
                Result<List<Query>, CommandResults, CString> result = buildNewQueries(ctx,
                        (SimpleSQLStatement) sqlStatement, toProcess);
                if (result.isQuery()) {
                    // Retrieve modified (or unmodified) queries to forward
                    newQueries = result.query();
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
                        session.setTransactionErrorDetails(errorDetails);
                    }
                    session.resetCurrentCommand();
                    newQueries = null;
                }
            } else {
                // Track parse step (for bind, describe and close steps)
                session.addParseStep((ParseStep) sqlStatement, operation, toProcess);
                // Postpone processing of extended query
                transferMode = TransferMode.FORGET;
                newQueries = null;
                response = new CommandResults();
                response.setParseCompleteRequired(true);
            }
        } else if (transferMode == TransferMode.FORGET) {
            // Buffer sql statement
            errorDetails = bufferQuery(ctx, sqlStatement);
            if (errorDetails != null) {
                transferMode = TransferMode.ERROR;
            }
            if (!(sqlStatement instanceof SimpleQuery)) {
                // Track parse step (for bind, describe and close steps)
                session.addParseStep((ParseStep) sqlStatement, operation, toProcess);
                response.setParseCompleteRequired(true);
            }
            newQueries = null;
        } else if (transferMode == TransferMode.ERROR) {
            if (session.getTransactionStatus() == (byte) 'T') {
                session.setTransactionErrorDetails(errorDetails);
            }
            session.resetCurrentCommand();
            newQueries = null;
        }
        QueriesTransferMode<SQLStatement, CommandResults> mode = new QueriesTransferMode<>(newQueries, transferMode,
                response, errorDetails);
        LOGGER.debug("SQL statement processed: new queries={}, transfer mode={}", mode.getNewQueries(),
                mode.getTransferMode());
        return mode;
    }

    private boolean isSQLStatementToProcess(Mode processingMode) {
        return FORCE_SQL_PROCESSING || processingMode != Mode.AS_IT_IS;
    }

    private Result<List<Query>, CommandResults, CString> buildNewQueries(ChannelHandlerContext ctx,
            SimpleSQLStatement sqlStatement, boolean toProcess) throws IOException {
        SimpleSQLStatement newSQLStatement = null;
        if (toProcess) {
            Result<SimpleSQLStatement, CommandResults, CString> result = processSimpleSQLStatement(ctx, sqlStatement);
            if (result.isQuery()) {
                newSQLStatement = result.query();
            } else if (result.isResponse()) {
                return Result.response(result.response());
            } else if (result.isError()) {
                return Result.error(result.error());
            }
        } else {
            newSQLStatement = sqlStatement;
        }
        List<Query> newQueries = processBufferedQueries(ctx);
        if (newQueries.isEmpty()) {
            if (newSQLStatement != null) {
                newQueries = Collections.singletonList(newSQLStatement);
            }
        } else {
            // Compute new SQL statement size (if buffered queries are all simple)
            boolean allSimple = true;
            int newSize = 0;
            for (Query newQuery : newQueries) {
                if (newQuery instanceof SimpleQuery) {
                    newSize += ((SQLStatement) newQuery).getSQL().length();
                } else {
                    allSimple = false;
                    break;
                }
            }
            if (allSimple) {
                if (newSQLStatement != null) {
                    newSize += newSQLStatement.getSQL().length();
                }
                CString newSQL = CString.valueOf(new StringBuilder(newSize));
                for (Query newQuery : newQueries) {
                    newSQL.append(((SQLStatement) newQuery).getSQL());
                }
                if (newSQLStatement != null) {
                    newSQL.append(newSQLStatement.getSQL());
                }
                newSQLStatement = new SimpleSQLStatement(newSQL);
                newQueries = Collections.singletonList(newSQLStatement);
                // Remove expected response messages
                SQLSession session = getSession(ctx);
                QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
                while (nextQueryResponseToIgnore == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
                    session.removeFirstQueryResponseToIgnore();
                    nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
                }
            } else if (newSQLStatement != null) {
                newQueries.add(newSQLStatement);
            }
        }
        return Result.query(newQueries);
    }

    private List<Query> processBufferedQueries(ChannelHandlerContext ctx) throws IOException {
        DataOperation dataOperation = null;
        SQLSession session = getSession(ctx);
        List<Statement> allStatements = new ArrayList<>(session.getBufferedQueries().size());
        List<List<ParameterValue>> allParameterValues = new ArrayList<>(session.getBufferedQueries().size());
        List<Integer> rows = new ArrayList<>(session.getBufferedQueries().size());
        List<Query> newQueries = session.getBufferedQueries();
        // Track indexes of parse steps (necessary for bind steps)
        Map<CString, Integer> lastParseStepIndexes = new HashMap<>();
        // Track usage of parse steps and bind steps to know if they must be closed
        Map<CString, Integer> parseStepCounters = session.getParseStepStatuses().keySet().stream()
                .collect(Collectors.toMap(java.util.function.Function.identity(), i -> 1)); // Initialize parse step counters
        Map<CString, Integer> bindStepCounters = session.getBindStepStatuses().keySet().stream()
                .collect(Collectors.toMap(java.util.function.Function.identity(), i -> 1)); // Initialize bind step counters
        int index = 0;
        int row = 0;
        for (Query bufferedQuery : session.getBufferedQueries()) {
            Statement stmt = null;
            List<ParameterValue> parameterValues = null;
            Integer optionalRow = null;
            boolean extract = false;
            if (bufferedQuery instanceof SimpleQuery) {
                // Parse SQL statement
                stmt = parseSQL(ctx, ((SimpleSQLStatement) bufferedQuery).getSQL());
                // Save row for this SQL statement
                optionalRow = Integer.valueOf(row++);
                // Ready to extract and protect data for this query
                extract = true;
            } else {
                ExtendedQuery query = (ExtendedQuery) bufferedQuery;
                if (query instanceof ParseStep) {
                    ParseStep parseStep = (ParseStep) query;
                    // Parse SQL statement
                    stmt = parseSQL(ctx, parseStep.getSQL());
                    // Save row for this SQL statement
                    optionalRow = Integer.valueOf(row); // Row incremented during bind step
                    // Track index of this parse step (necessary for bind step)
                    lastParseStepIndexes.put(parseStep.getName(), index);
                    // Increment parse step counter
                    Integer counter = parseStepCounters.get(parseStep.getName());
                    parseStepCounters.put(parseStep.getName(), counter + 1);
                } else if (query instanceof BindStep) {
                    // Extract parameter values
                    BindStep bindStep = (BindStep) query;
                    ParseStep parseStep = null;
                    if (lastParseStepIndexes.containsKey(bindStep.getPreparedStatement())) {
                        // Parse step was within buffered queries
                        int lastParseStepIndex = lastParseStepIndexes.get(bindStep.getPreparedStatement());
                        // Retrieve parse step from buffered queries
                        parseStep = (ParseStep) session.getBufferedQueries().get(lastParseStepIndex);
                        // SQL statement was already parsed
                        stmt = allStatements.get(lastParseStepIndex);
                    } else {
                        // Parse step was done long time ago (and is tracked by session)
                        ExtendedQueryStatus<ParseStep> parseStepStatus = session
                                .getParseStepStatus(bindStep.getPreparedStatement());
                        if (parseStepStatus != null) {
                            parseStep = parseStepStatus.getQuery();
                            // Parse SQL statement
                            stmt = parseSQL(ctx, parseStep.getSQL());
                        }
                    }
                    if (parseStep != null) {
                        // Build portal parameter (type+format+value)
                        parameterValues = bindStep.getParameterValues().stream().map(ParameterValue::new) // create a PortalParameter for each parameter value
                                .collect(Collectors.toList()); // build a list
                        final List<Long> parameterTypes = parseStep.getParameterTypes();
                        for (int idx = 0; idx < parameterTypes.size(); idx++) {
                            // set parameter type
                            parameterValues.get(idx).setType(parameterTypes.get(idx));
                        }
                        final List<Short> formats = bindStep.getParameterFormats();
                        for (int i = 0; i < parameterValues.size(); i++) {
                            ParameterValue parameterValue = parameterValues.get(i);
                            Short format = formats.get(formats.size() == 1 ? 0 : i);
                            // set parameter value format
                            parameterValue.setFormat(format);
                        }
                        // Save row for this bind step
                        optionalRow = Integer.valueOf(row++);
                        // Ready to extract and protect data for this query
                        extract = true;
                    }
                    // Increment bind step counter
                    Integer counter = bindStepCounters.get(bindStep.getName());
                    bindStepCounters.put(bindStep.getName(), counter + 1);
                } else if (query instanceof DescribeStep) {
                    // nothing to do, just send as-it-is
                } else if (query instanceof ExecuteStep) {
                    // nothing to do, just send as-it-is
                } else if (query instanceof CloseStep) {
                    CloseStep closeStep = (CloseStep) query;
                    if (closeStep.getCode() == 'S') {
                        // Decrement parse step counter
                        Integer counter = parseStepCounters.get(closeStep.getName());
                        parseStepCounters.put(closeStep.getName(), counter - 1);
                    } else {
                        // Decrement bind step counter
                        Integer counter = bindStepCounters.get(closeStep.getName());
                        bindStepCounters.put(closeStep.getName(), counter - 1);
                    }
                } else if (query instanceof SynchronizeStep) {
                    // nothing to do, just send as-it-is
                } else if (query instanceof FlushStep) {
                    // nothing to do, just send as-it-is
                }
            }
            allStatements.add(stmt);
            allParameterValues.add(parameterValues);
            rows.add(optionalRow);
            if (extract) {
                // Extract module operation
                ModuleOperation moduleOperation = null;
                // Extract data operation
                try {
                    if (stmt instanceof CreateTable) {
                        moduleOperation = extractCreateTableOperation(ctx, (CreateTable) stmt);
                    } else if (stmt instanceof Insert) {
                        moduleOperation = extractInsertOperation(ctx, (Insert) stmt, parameterValues, dataOperation);
                    } else if (stmt instanceof Select) {
                        moduleOperation = extractSelectOperation(ctx, (Select) stmt, parameterValues);
                    }
                } catch (ParseException e) {
                    // Should not occur (unsupported metadata in buffered queries)
                    throw new IOException(String.format("%s is unsupported in buffered queries", FUNCTION_METADATA), e);
                }
                if (moduleOperation instanceof DataOperation) {
                    dataOperation = (DataOperation) moduleOperation;
                } else {
                    // Should not occur (unsupported metadata in buffered queries)
                    throw new IOException(String.format("%s is unsupported in buffered queries", FUNCTION_METADATA));
                }
            }
            index++;
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
        // Process data operation
        if (dataOperation != null) {
            List<DataOperation> newDataOperations = getProtocolService(ctx).newDataOperation(dataOperation);
            dataOperation = newDataOperations.get(0);
            if (dataOperation.isModified()) {
                // Modify SQL queries (and parameter values)
                newQueries = new ArrayList<>(session.getBufferedQueries().size());
                index = 0;
                for (Query bufferedQuery : session.getBufferedQueries()) {
                    Statement stmt = allStatements.get(index);
                    Query newQuery = bufferedQuery;
                    if (stmt != null) {
                        List<ParameterValue> parameterValues = allParameterValues.get(index);
                        // Modify SQL statement
                        Integer optionalRow = rows.get(index);
                        if (optionalRow != null) {
                            if (stmt instanceof Insert) {
                                modifyInsertStatement(ctx, (Insert) stmt, parameterValues, dataOperation, optionalRow);
                            } else if (stmt instanceof Select) {
                                modifySelectStatement(ctx, (Select) stmt, parameterValues, dataOperation, optionalRow);
                            }
                        }
                        if (bufferedQuery instanceof SimpleQuery) {
                            SimpleSQLStatement simpleSQLStatement = (SimpleSQLStatement) bufferedQuery;
                            String newSQL = stmt.toString();
                            newSQL = StringUtilities.addIrrelevantCharacters(newSQL, simpleSQLStatement.getSQL(),
                                    " \t\r\n;");
                            newQuery = new SimpleSQLStatement(CString.valueOf(newSQL));
                        } else {
                            ExtendedQuery query = (ExtendedQuery) bufferedQuery;
                            if (query instanceof ParseStep) {
                                ParseStep parseStep = (ParseStep) query;
                                String newSQL = stmt.toString();
                                newSQL = StringUtilities.addIrrelevantCharacters(newSQL, parseStep.getSQL(),
                                        " \t\r\n;");
                                newQuery = new ParseStep(parseStep.getName(), CString.valueOf(newSQL),
                                        parseStep.isMetadata(), parseStep.getColumns(), parseStep.getParameterTypes());
                            } else if (query instanceof BindStep) {
                                BindStep bindStep = (BindStep) query;
                                List<ByteBuf> parameterBinaryValues = parameterValues.stream()
                                        .map(ParameterValue::getValue) // get the parameter value
                                        .collect(Collectors.toList()); // build a list
                                if (!parameterBinaryValues.equals(bindStep.getParameterValues())) {
                                    // At least one parameter ByteBuf values has been changed
                                    newQuery = new BindStep(bindStep.getName(), bindStep.getPreparedStatement(),
                                            bindStep.getParameterFormats(), parameterBinaryValues,
                                            bindStep.getResultColumnFormats());
                                }
                            }
                        }
                    }
                    newQuery.retain();
                    newQueries.add(newQuery);
                    index++;
                }
            }
        }
        session.resetBufferedQueries();
        return newQueries;
    }

    private static class Result<Q, R, E> {
        private final Optional<Q> query;
        private final Optional<R> response;
        private final Optional<E> error;

        public static <Q, R, E> Result<Q, R, E> query(Q query) {
            return new Result<>(Optional.ofNullable(query), null, null);
        }

        public static <Q, R, E> Result<Q, R, E> response(R response) {
            return new Result<>(null, Optional.ofNullable(response), null);
        }

        public static <Q, R, E> Result<Q, R, E> error(E error) {
            return new Result<>(null, null, Optional.ofNullable(error));
        }

        private Result(Optional<Q> query, Optional<R> response, Optional<E> error) {
            this.query = query;
            this.response = response;
            this.error = error;
        }

        public Q query() {
            return query == null ? null : query.orElse(null);
        }

        public R response() {
            return response == null ? null : response.orElse(null);
        }

        public E error() {
            return error == null ? null : error.orElse(null);
        }

        public boolean isQuery() {
            return query != null;
        }

        public boolean isResponse() {
            return response != null;
        }

        public boolean isError() {
            return error != null;
        }
    }

    private Result<SimpleSQLStatement, CommandResults, CString> processSimpleSQLStatement(ChannelHandlerContext ctx,
            SimpleSQLStatement sqlStatement) {
        // Parse SQL statement
        Statement stmt = parseSQL(ctx, sqlStatement.getSQL());
        if (stmt == null) {
            return Result.query(sqlStatement);
        }
        // Extract module operation
        ModuleOperation moduleOperation = null;
        try {
            if (stmt instanceof CreateTable) {
                moduleOperation = extractCreateTableOperation(ctx, (CreateTable) stmt);
            } else if (stmt instanceof Insert) {
                moduleOperation = extractInsertOperation(ctx, (Insert) stmt, null, null);
            } else if (stmt instanceof Select) {
                moduleOperation = extractSelectOperation(ctx, (Select) stmt, null);
            } else if (stmt instanceof Update) {
                // TODO Update
            } else if (stmt instanceof Delete) {
                // TODO Delete
            }
        } catch (ParseException e) {
            return Result.error(CString.valueOf(e.getMessage()));
        }
        Result<SimpleSQLStatement, CommandResults, CString> result = null;
        if (moduleOperation instanceof MetadataOperation) {
            // Process metadata operation
            MetadataOperation metadataOperation = (MetadataOperation) moduleOperation;
            metadataOperation = newMetaDataOperation(ctx, metadataOperation);
            // Forget SQL statement, reply directly to the frontend
            CommandResults commandResults = new CommandResults();
            // Build row description
            List<PgsqlRowDescriptionMessage.Field> description = Stream.of("column_name", "protected_column_name")
                    .map(CString::valueOf).map(PgsqlRowDescriptionMessage.Field::new).collect(Collectors.toList());
            commandResults.setRowDescription(description);
            if (metadataOperation.isModified()) {
                Map<CString, List<CString>> metadata = metadataOperation.getMetadata();
                // Verify all data ids refer to the same dataset (prefix is the same for all data ids)
                Set<CString> prefixes = metadata.keySet().stream().map(id -> id.substring(0, id.lastIndexOf('/')))
                        .collect(Collectors.toSet());
                boolean multipleDatasets = prefixes.size() > 1 || prefixes.stream().findFirst().get().equals("*");
                if (!multipleDatasets) {
                    // Prefix is the same for all data ids -> remove prefix
                    metadata = metadata.entrySet().stream().collect(Collectors
                            .toMap(e -> e.getKey().substring(e.getKey().lastIndexOf('/') + 1), Map.Entry::getValue));
                }
                // Build rows
                List<List<ByteBuf>> rows = buildRows(metadata, -1);
                commandResults.setRows(rows);
                // Build complete tag
                commandResults.setCompleteTag(CString.valueOf("SELECT " + rows.size()));
            } else {
                commandResults.setCompleteTag(CString.valueOf("SELECT 0"));
            }
            result = Result.response(commandResults);
        } else if (moduleOperation instanceof DataOperation) {
            // Process data operation
            DataOperation dataOperation = (DataOperation) moduleOperation;
            List<DataOperation> newDataOperations = getProtocolService(ctx).newDataOperation(dataOperation);
            dataOperation = newDataOperations.get(0);
            if (dataOperation.isModified()) {
                // Modify SQL statement
                if (stmt instanceof Insert) {
                    modifyInsertStatement(ctx, (Insert) stmt, null, dataOperation, 0);
                } else if (stmt instanceof Select) {
                    modifySelectStatement(ctx, (Select) stmt, null, dataOperation, 0);
                } else if (stmt instanceof Update) {
                    // TODO Update
                } else if (stmt instanceof Delete) {
                    // TODO Delete
                }
                String newSQL = stmt.toString();
                newSQL = StringUtilities.addIrrelevantCharacters(newSQL, sqlStatement.getSQL(), " \t\r\n;");
                sqlStatement = new SimpleSQLStatement(CString.valueOf(newSQL));
            }
            SQLSession session = getSession(ctx);
            if (session.getCurrentCommandOperation() == Operation.READ) {
                session.setPromise(dataOperation.getPromise());
                session.setResultProcessingEnabled(dataOperation.isResultProcessingEnabled());
                session.setInvolvedCSPs(dataOperation.getInvolvedCSPs());
                session.setDataIds(dataOperation.getDataIds());
            }
            result = Result.query(sqlStatement);
        } else {
            result = Result.query(sqlStatement);
        }
        return result;
    }

    private MetadataOperation newMetaDataOperation(ChannelHandlerContext ctx, MetadataOperation metadataOperation) {
        MetadataOperation result = getProtocolService(ctx).newMetadataOperation(metadataOperation);
        // Remove any duplicate data id (the data ids that refer to the public schema)
        Pattern publicDataIdPattern = Pattern.compile("([^/]*/)(public\\.)([^/\\.]*/[^/]*)");
        List<CString> dataIds = result.getDataIds();
        dataIds.stream().filter(id -> {
            Matcher m = publicDataIdPattern.matcher(id.toString());
            if (m.matches()) {
                String target = m.replaceAll("$1$3");
                return dataIds.stream().map(CString::toString).anyMatch(Predicate.isEqual(target));
            } else {
                return false;
            }
        }).forEach(id -> result.removeDataId(id));
        return result;
    }

    private DataOperation extractCreateTableOperation(ChannelHandlerContext ctx, CreateTable stmt)
            throws ParseException {
        // TODO not yet supported. We just extract table definition
        // Extract dataset id
        String schemaId = getSchemaId(ctx, stmt.getTable());
        String datasetId = getDatasetId(ctx, stmt.getTable(), schemaId);
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
                // build a list
                .collect(Collectors.toList());
        // TODO modification not yet supported. We just extract table definition
        SQLSession session = getSession(ctx);
        session.addDatasetDefinition(CString.valueOf(datasetId), dataIds);
        return null;
    }

    private DataOperation extractInsertOperation(ChannelHandlerContext ctx, Insert stmt,
            List<ParameterValue> parameterValues, DataOperation dataOperation) throws ParseException {
        if (dataOperation == null) {
            dataOperation = new DataOperation();
            dataOperation.setOperation(Operation.CREATE);
            // Extract dataset id
            String schemaId = getSchemaId(ctx, stmt.getTable());
            String datasetId = getDatasetId(ctx, stmt.getTable(), schemaId);
            // Extract data ids
            List<CString> dataIds;
            if (stmt.getColumns() == null) {
                SQLSession session = getSession(ctx);
                dataIds = session.getDatasetDefinition(CString.valueOf(datasetId));
                if (dataIds == null) {
                    dataIds = Collections.emptyList();
                    MetadataOperation metadataOperation = new MetadataOperation();
                    metadataOperation.addDataId(CString.valueOf(datasetId + "*"));
                    dataIds = newMetaDataOperation(ctx, metadataOperation).getDataIds();
                }
            } else {
                dataIds = stmt.getColumns().stream().map(Column::getColumnName) // get column name
                        .map(StringUtilities::unquote) // unquote string
                        .map(cn -> datasetId + cn) // build dataId
                        .map(CString::valueOf) // transform to CString
                        .collect(Collectors.toList()); // build a list
            }
            dataOperation.setDataIds(dataIds);
        }
        // Extract data values
        List<ExpressionList> rows = null;
        if (stmt.getItemsList() instanceof ExpressionList) {
            rows = Collections.singletonList((ExpressionList) stmt.getItemsList());
        } else if (stmt.getItemsList() instanceof MultiExpressionList) {
            rows = ((MultiExpressionList) stmt.getItemsList()).getExprList();
        }
        // TODO more complex insert statement
        //      } else if (stmt.getItemsList() instanceof SubSelect) {
        //      }
        if (rows != null) {
            for (ExpressionList row : rows) {
                List<CString> dataValues = row.getExpressions().stream()
                        .map(exp -> exp instanceof NullValue ? null : exp.toString()) // transform to string
                        .map(CString::valueOf) // transform to CString
                        .map(value -> {
                            if (parameterValues != null) {
                                if (value != null && value.charAt(0) == '$') { // if value is a parameter id ($<index>)
                                    int idx = Integer.parseInt(value.substring(1).toString()) - 1; // get the parameter index
                                    ParameterValue parameterValue = parameterValues.get(idx); // get the parameter value
                                    value = convertToText(parameterValue.getType(), // convert parameter value to string
                                            parameterValue.getFormat(), parameterValue.getValue());
                                }
                            }
                            return value;
                        }).collect(Collectors.toList()); // build a list
                dataOperation.addDataValues(dataValues);
            }
        }
        return dataOperation;
    }

    private void modifyInsertStatement(ChannelHandlerContext ctx, Insert stmt, List<ParameterValue> parameterValues,
            DataOperation dataOperation, int row) {
        if (stmt.getItemsList() instanceof ExpressionList) {
            final List<Expression> expressions = ((ExpressionList) stmt.getItemsList()).getExpressions();
            List<CString> dataValues = dataOperation.getDataValues().get(row);
            for (int i = 0; i < dataValues.size(); i++) {
                Expression expression = expressions.get(i);
                if (!(expression instanceof NullValue)) {
                    String strValue = expression.toString();
                    if (strValue.charAt(0) == '$') {
                        // modify parameter
                        if (parameterValues != null) {
                            int paramIndex = Integer.parseInt(strValue.substring(1)) - 1; // get the parameter index
                            ParameterValue parameterValue = parameterValues.get(paramIndex); // get the parameter value
                            ByteBuf value = convertToByteBuf(parameterValue.getType(), // convert string to ByteBuf
                                    parameterValue.getFormat(), dataValues.get(i));
                            // modify parameter value
                            parameterValue.setValue(value);
                        }
                    } else {
                        // modify expression
                        expressions.set(i, new StringValue(dataValues.get(i).toString()));
                    }
                }
            }
        } else if (stmt.getItemsList() instanceof MultiExpressionList) {
            final List<ExpressionList> expressionLists = ((MultiExpressionList) stmt.getItemsList()).getExprList();
            for (ExpressionList expressionList : expressionLists) {
                final List<Expression> expressions = expressionList.getExpressions();
                List<CString> dataValues = dataOperation.getDataValues().get(row ++);
                for (int i = 0; i < dataValues.size(); i++) {
                    Expression expression = expressions.get(i);
                    if (!(expression instanceof NullValue)) {
                        String strValue = expression.toString();
                        if (strValue.charAt(0) == '$') {
                            // modify parameter
                            if (parameterValues != null) {
                                int paramIndex = Integer.parseInt(strValue.substring(1)) - 1; // get the parameter index
                                ParameterValue parameterValue = parameterValues.get(paramIndex); // get the parameter value
                                ByteBuf value = convertToByteBuf(parameterValue.getType(), // convert string to ByteBuf
                                        parameterValue.getFormat(), dataValues.get(i));
                                // modify parameter value
                                parameterValue.setValue(value);
                            }
                        } else {
                            // modify expression
                            expressions.set(i, new StringValue(dataValues.get(i).toString()));
                        }
                    }
                }
            }
        } else if (stmt.getItemsList() instanceof SubSelect) {
            // TODO more complex insert statement
        }
    }

    private ModuleOperation extractSelectOperation(ChannelHandlerContext ctx, Select stmt,
            List<ParameterValue> parameterValues) throws ParseException {
        if (!(stmt.getSelectBody() instanceof PlainSelect)) {
            return null;
        }
        // Extract dataset id
        PlainSelect select = (PlainSelect) stmt.getSelectBody();
        int capacity = (select.getFromItem() instanceof Table ? 1 : 0)
                + (select.getJoins() != null ? select.getJoins().size() : 0);
        List<String> schemaIds = new ArrayList<>(capacity);
        List<String> datasetIds = new ArrayList<>(capacity);
        if (select.getFromItem() instanceof Table) {
            Table table = (Table) select.getFromItem();
            String schemaId = getSchemaId(ctx, table);
            schemaIds.add(schemaId);
            String datasetId = getDatasetId(ctx, table, schemaId);
            datasetIds.add(datasetId);
            if (select.getJoins() != null) {
                for (Join join : select.getJoins()) {
                    if (join.getRightItem() instanceof Table) {
                        table = (Table) join.getRightItem();
                        schemaId = getSchemaId(ctx, table);
                        schemaIds.add(schemaId);
                        datasetId = getDatasetId(ctx, table, schemaId);
                        datasetIds.add(datasetId);
                    }
                }
            }
        } else {
            String schemaId = getSchemaId(ctx, null);
            schemaIds.add(schemaId);
            String datasetId = getDatasetId(ctx, null, schemaId);
            datasetIds.add(datasetId);
        }
        // Extract data ids
        List<CString> dataIds = new ArrayList<>();
        boolean metadata = false;
        List<CString> involvedCSPs = null;
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
                            datasetIds.forEach(id -> metadataOperation.addDataId(CString.valueOf(id + "*")));
                            dataIds = newMetaDataOperation(ctx, metadataOperation).getDataIds();
                        } else if (function.getParameters() != null && function.getParameters().getExpressions() != null
                                && !function.getParameters().getExpressions().isEmpty()) {
                            for (Expression parameter : function.getParameters().getExpressions()) {
                                if (parameter instanceof Column) {
                                    Column column = (Column) parameter;
                                    if (column.getTable() != null && column.getTable().getName() != null) {
                                        dataIds.addAll(
                                                schemaIds.stream()
                                                        .map(id -> CString.valueOf(id + column.getTable().getName()
                                                                + "/" + column.getColumnName()))
                                                        .collect(Collectors.toList()));
                                    } else {
                                        dataIds.addAll(datasetIds.stream()
                                                .map(id -> CString.valueOf(id + column.getColumnName()))
                                                .collect(Collectors.toList()));
                                    }
                                }
                            }
                        } else {
                            // Error: expected at least one parameter (* or column name)
                            throw new ParseException(FUNCTION_METADATA
                                    + " function requires at least one parameter (* or column name(s))");
                        }
                        break;
                    } else if (FUNCTION_PROTECTED.equalsIgnoreCase(function.getName())) {
                        if (function.getParameters() != null && function.getParameters().getExpressions() != null
                                && !function.getParameters().getExpressions().isEmpty()) {
                            involvedCSPs = new ArrayList<>(function.getParameters().getExpressions().size());
                            for (Expression parameter : function.getParameters().getExpressions()) {
                                if (parameter instanceof StringValue) {
                                    involvedCSPs.add(CString.valueOf(StringUtilities.unquote(parameter.toString())));
                                } else {
                                    // Error: expected at least one parameter (csp names)
                                    throw new ParseException(
                                            FUNCTION_PROTECTED + " function requires string parameters (CSP names)");
                                }
                            }
                        } else {
                            // Error: expected at least one parameter (csp names)
                            throw new ParseException(
                                    FUNCTION_PROTECTED + " function requires at least one parameter (CSP names)");
                        }
                        iter.remove();
                    } else {
                        dataIds.add(CString.valueOf(function.getName()));
                    }
                } else {
                    String unquote = StringUtilities.unquote(selectExpressionItem.getExpression().toString());
                    dataIds.addAll(
                            datasetIds.stream().map(id -> CString.valueOf(id + unquote)).collect(Collectors.toList()));
                }
            } else if (selectItem instanceof AllColumns || selectItem instanceof AllTableColumns) {
                MetadataOperation metadataOperation = new MetadataOperation();
                if (selectItem instanceof AllColumns) {
                    datasetIds.forEach(id -> metadataOperation
                            .addDataId(CString.valueOf(id + ((AllColumns) selectItem).toString())));
                } else /* if (selectItem instanceof AllTableColumns) */ {
                    schemaIds.forEach(id -> metadataOperation.addDataId(
                            CString.valueOf(id + ((AllTableColumns) selectItem).toString().replace('.', '/'))));
                }
                dataIds = newMetaDataOperation(ctx, metadataOperation).getDataIds();
            }
        }
        ModuleOperation moduleOperation;
        if (metadata) {
            MetadataOperation metadataOperation = new MetadataOperation();
            metadataOperation.setDataIds(dataIds);
            moduleOperation = metadataOperation;
        } else {
            DataOperation dataOperation = new DataOperation();
            dataOperation.setOperation(Operation.READ);
            dataOperation.setDataIds(dataIds);
            // Extract parameter ids and values (functions)
            for (SelectItem selectItem : select.getSelectItems()) {
                if (selectItem instanceof SelectExpressionItem
                        && ((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
                    Function function = (Function) ((SelectExpressionItem) selectItem).getExpression();
                    if (function.getParameters() != null) {
                        dataOperation.addParameterId(CString.valueOf(function.getName()));
                        CString value = CString.valueOf(
                                PlainSelect.getStringList(function.getParameters().getExpressions(), true, false));
                        dataOperation.addParameterValue(value);
                    }
                }
            }
            // TODO Extract parameter ids and values (clause where)
            // Disable result processing if needed
            if (involvedCSPs != null) {
                dataOperation.setResultProcessingEnabled(false);
                dataOperation.setInvolvedCSPs(involvedCSPs);
                dataOperation.setModified(true);
            }
            moduleOperation = dataOperation;
        }
        return moduleOperation;
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
            SQLSession session = getSession(ctx);
            if (session.getDatabaseName() != null) {
                sb.append(session.getDatabaseName());
            }
        }
        if (sb.length() == 0) {
            sb.append('*');
        }
        sb.append('/');
        if (table != null) {
            String schemaName = StringUtilities.unquote(table.getSchemaName());
            if (schemaName != null) {
                sb.append(schemaName.toLowerCase()).append('.');
            }
        }
        String schemaId = sb.toString();
        return schemaId;
    }

    private String getDatasetId(ChannelHandlerContext ctx, Table table, String schemaId) {
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

    private void modifySelectStatement(ChannelHandlerContext ctx, Select stmt, List<ParameterValue> parameterValues,
            DataOperation dataOperation, int row) {
        PlainSelect select = (PlainSelect) stmt.getSelectBody();
        for (SelectItem selectItem : select.getSelectItems()) {
            if (selectItem instanceof SelectExpressionItem
                    && ((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
                Function function = (Function) ((SelectExpressionItem) selectItem).getExpression();
                int index = dataOperation.getParameterIds().indexOf(CString.valueOf(function.getName()));
                if (index != -1) {
                    String expression = dataOperation.getParameterValues().get(index).toString();
                    CCJSqlParser parser = new CCJSqlParser(new StringReader(expression));
                    try {
                        ExpressionList parameters = parser.SimpleExpressionList();
                        function.setParameters(parameters);
                    } catch (Exception e) {
                        LOGGER.error("Parsing error for {} : ", expression, e);
                    }
                }
            }
        }
        // TODO Modify parameter values (clause where)
    }

    @Override
    public QueriesTransferMode<BindStep, CommandResults> processBindStep(ChannelHandlerContext ctx, BindStep bindStep)
            throws IOException {
        LOGGER.debug("Bind step: {}", bindStep);
        SQLSession session = getSession(ctx);
        ExtendedQueryStatus<ParseStep> parseStepStatus = session.getParseStepStatus(bindStep.getPreparedStatement());
        if (parseStepStatus == null) {
            throw new IllegalStateException(
                    String.format("Parse step not found for bind step '%s'", bindStep.getName()));
        }
        TransferMode transferMode = TransferMode.FORWARD;
        List<Query> newQueries = Collections.singletonList(bindStep);
        CommandResults response = null;
        Map<Byte, CString> errorDetails = null;
        // Track bind step (for describe, execute and close steps)
        ExtendedQueryStatus<BindStep> bindStepStatus = session.addBindStep(bindStep, parseStepStatus.getOperation(),
                parseStepStatus.isToProcess());
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
                    // TODO orchestration mode. Meanwhile, same as AS_IT_IS mode
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
                // TODO detect if delete is on the whole dataset
                boolean deleteWholeDataset = false;
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
        session.setTransferMode(transferMode);
        if (transferMode == TransferMode.FORWARD) {
            Result<List<Query>, CommandResults, CString> result = buildNewQueries(ctx, parseStepStatus, bindStepStatus);
            if (result.isQuery()) {
                // Retrieve modified (or unmodified) queries to forward
                newQueries = result.query();
                for (Query query : newQueries) {
                    if (query instanceof ParseStep) {
                        // Parse is forwarded before bind -> response must be ignored
                        session.addLastQueryResponseToIgnore(QueryResponseType.PARSE_COMPLETE);
                    } else if (query instanceof DescribeStep && ((DescribeStep) query).getCode() == 'S') {
                        // Describe is forwarded before bind -> response must be ignored
                        session.addLastQueryResponseToIgnore(QueryResponseType.PARAMETER_DESCRIPTION);
                        if (parseStepStatus.getOperation() == Operation.READ) {
                            session.addLastQueryResponseToIgnore(QueryResponseType.ROW_DESCRIPTION_AND_ROW_DATA);
                        } else {
                            session.addLastQueryResponseToIgnore(QueryResponseType.NO_DATA);
                        }
                    }
                }
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
                    session.setTransactionErrorDetails(errorDetails);
                }
                session.resetCurrentCommand();
                newQueries = null;
            }
        } else if (transferMode == TransferMode.FORGET) {
            // Buffer extended query
            errorDetails = bufferQuery(ctx, bindStep);
            if (errorDetails != null) {
                transferMode = TransferMode.ERROR;
            } else {
                response = new CommandResults();
                response.setBindCompleteRequired(true);
            }
            newQueries = null;
        } else if (transferMode == TransferMode.ERROR) {
            if (session.getTransactionStatus() == (byte) 'T') {
                session.setTransactionErrorDetails(errorDetails);
            }
            session.resetCurrentCommand();
            newQueries = null;
        }
        QueriesTransferMode<BindStep, CommandResults> mode = new QueriesTransferMode<>(newQueries, transferMode,
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
        List<Query> newQueries = Collections.singletonList(describeStep);
        CommandResults response = null;
        Map<Byte, CString> errorDetails = null;
        if (transferMode == TransferMode.FORWARD) {
            if (describeStep.getCode() == 'S') {
                // Postpone processing of extended query
                session.addDescribeStep(describeStep);
                transferMode = TransferMode.FORGET;
                newQueries = null;
            } else if (describeStep.getCode() == 'P') {
                ExtendedQueryStatus<BindStep> bindStepStatus = session.getBindStepStatus(describeStep.getName());
                BindStep bindStep = bindStepStatus.getQuery();
                ExtendedQueryStatus<ParseStep> parseStepStatus = session
                        .getParseStepStatus(bindStep.getPreparedStatement());
                ParseStep parseStep = parseStepStatus.getQuery();
                if (parseStep.isMetadata()) {
                    transferMode = TransferMode.FORGET;
                    newQueries = null;
                } else {
                    // nothing to do, just forward query
                }
            }
        } else if (transferMode == TransferMode.FORGET) {
            // Buffer extended query
            errorDetails = bufferQuery(ctx, describeStep);
            if (errorDetails != null) {
                transferMode = TransferMode.ERROR;
            }
            newQueries = null;
        } else if (transferMode == TransferMode.ERROR) {
            errorDetails = session.getRetainedTransactionErrorDetails();
            session.resetCurrentCommand();
            newQueries = null;
        }
        if (transferMode == TransferMode.FORGET) {
            // Build response
            response = new CommandResults();
            ParseStep parseStep = null;
            if (describeStep.getCode() == 'S') {
                ExtendedQueryStatus<ParseStep> parseStepStatus = session.getParseStepStatus(describeStep.getName());
                parseStep = parseStepStatus.getQuery();
                // TODO parse SQL statement to extract exact list of parameter
                List<Long> parameterTypes = new ArrayList<Long>(parseStep.getParameterTypes());
                response.setParameterDescription(parameterTypes);
            } else if (describeStep.getCode() == 'P') {
                ExtendedQueryStatus<BindStep> bindStepStatus = session.getBindStepStatus(describeStep.getName());
                BindStep bindStep = bindStepStatus.getQuery();
                ExtendedQueryStatus<ParseStep> parseStepStatus = session
                        .getParseStepStatus(bindStep.getPreparedStatement());
                parseStep = parseStepStatus.getQuery();
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
                // TODO parse SQL statement to extract selected columns (to row description)
                rowDescription = Collections.emptyList(); // empty list means no data
            }
            response.setRowDescription(rowDescription);
        }
        QueriesTransferMode<DescribeStep, CommandResults> mode = new QueriesTransferMode<>(newQueries, transferMode,
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
        List<Query> newQueries = Collections.singletonList(executeStep);
        CommandResults response = null;
        Map<Byte, CString> errorDetails = null;
        if (transferMode == TransferMode.FORWARD) {
            ExtendedQueryStatus<BindStep> bindStepStatus = session.getBindStepStatus(executeStep.getPortal());
            BindStep bindStep = bindStepStatus.getQuery();
            ExtendedQueryStatus<ParseStep> parseStepStatus = session
                    .getParseStepStatus(bindStep.getPreparedStatement());
            ParseStep parseStep = parseStepStatus.getQuery();
            if (parseStep.isMetadata()) {
                transferMode = TransferMode.FORGET;
                // Process metadata operation
                MetadataOperation metadataOperation = new MetadataOperation();
                metadataOperation.setDataIds(parseStep.getColumns());
                metadataOperation = newMetaDataOperation(ctx, metadataOperation);
                // Forget SQL statement, reply directly to the frontend
                response = new CommandResults();
                if (metadataOperation.isModified()) {
                    Map<CString, List<CString>> metadata = metadataOperation.getMetadata();
                    // Verify all data ids refer to the same dataset (prefix is the same for all data ids)
                    Set<CString> prefixes = metadata.keySet().stream().map(id -> id.substring(0, id.lastIndexOf('/')))
                            .collect(Collectors.toSet());
                    boolean multipleDatasets = prefixes.size() > 1 || prefixes.stream().findFirst().get().equals("*");
                    if (!multipleDatasets) {
                        // Prefix is the same for all data ids -> remove prefix
                        metadata = metadata.entrySet().stream().collect(Collectors.toMap(
                                e -> e.getKey().substring(e.getKey().lastIndexOf('/') + 1), Map.Entry::getValue));
                    }
                    // Build rows
                    List<List<ByteBuf>> rows = buildRows(metadata, executeStep.getMaxRows());
                    response.setRows(rows);
                    // Build complete tag
                    response.setCompleteTag(CString.valueOf("SELECT " + rows.size()));
                } else {
                    // Build complete tag
                    response.setCompleteTag(CString.valueOf("SELECT 0"));
                }
                newQueries = null;
            } else {
                // nothing to do, just forward query
            }
        } else if (transferMode == TransferMode.FORGET) {
            ExtendedQueryStatus<BindStep> bindStepStatus = session.getBindStepStatus(executeStep.getPortal());
            Operation operation = bindStepStatus.getOperation();
            if (operation != null) {
                response = new CommandResults();
                switch (operation) {
                case READ:
                    response.setCompleteTag(CString.valueOf("SELECT 0"));
                    break;
                case CREATE:
                    response.setCompleteTag(CString.valueOf("INSERT 0 1"));
                    break;
                case UPDATE:
                    response.setCompleteTag(CString.valueOf("UPDATE 0 1"));
                    break;
                case DELETE:
                    response.setCompleteTag(CString.valueOf("DELETE 0 1"));
                    break;
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
        } else if (transferMode == TransferMode.ERROR) {
            errorDetails = session.getRetainedTransactionErrorDetails();
            session.resetCurrentCommand();
            newQueries = null;
        }
        QueriesTransferMode<ExecuteStep, CommandResults> mode = new QueriesTransferMode<>(newQueries, transferMode,
                response, errorDetails);
        LOGGER.debug("Execute step processed: new queries={}, transfer mode={}", mode.getNewQueries(),
                mode.getTransferMode());
        return mode;
    }

    private List<List<ByteBuf>> buildRows(Map<CString, List<CString>> metadata, int maxSize) {
        // Build rows (replacing / by .)
        List<List<ByteBuf>> rows = metadata.entrySet().stream().flatMap(e -> {
            CString key = e.getKey().replace('/', '.');
            ByteBuf clearColumn = key.getByteBuf(key.length());
            return e.getValue().stream().map(v -> {
                CString value = v != null ? v.replace('/', '.') : null;
                ByteBuf protectedColumn = value != null ? value.getByteBuf(value.length()) : null;
                return Stream.of(clearColumn, protectedColumn).collect(Collectors.toList());
            });
        }).limit(maxSize < 0 ? Integer.MAX_VALUE : maxSize).collect(Collectors.toList());
        return rows;
    }

    @Override
    public QueriesTransferMode<CloseStep, CommandResults> processCloseStep(ChannelHandlerContext ctx,
            CloseStep closeStep) throws IOException {
        LOGGER.debug("Close step: {}", closeStep);
        SQLSession session = getSession(ctx);
        TransferMode transferMode = session.getTransferMode();
        List<Query> newQueries = Collections.singletonList(closeStep);
        CommandResults response = null;
        Map<Byte, CString> errorDetails = null;
        if (transferMode == TransferMode.FORWARD) {
            if (closeStep.getCode() == 'S') {
                ExtendedQueryStatus<ParseStep> parseStepStatus = session.getParseStepStatus(closeStep.getName());
                ParseStep parseStep = parseStepStatus.getQuery();
                if (parseStep.isMetadata()) {
                    transferMode = TransferMode.FORGET;
                    response = new CommandResults();
                    response.setCloseCompleteRequired(true);
                    newQueries = null;
                }
                session.removeParseStep(closeStep.getName());
            } else {
                session.removeBindStep(closeStep.getName());
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
            errorDetails = session.getRetainedTransactionErrorDetails();
            session.resetCurrentCommand();
            newQueries = null;
        }
        QueriesTransferMode<CloseStep, CommandResults> mode = new QueriesTransferMode<>(newQueries, transferMode,
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
        List<Query> newQueries = Collections.singletonList(synchronizeStep);
        Byte response = null;
        Map<Byte, CString> errorDetails = null;
        if (transferMode == TransferMode.FORGET) {
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
        }
        QueriesTransferMode<SynchronizeStep, Byte> mode = new QueriesTransferMode<>(newQueries, transferMode, response,
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
        List<Query> newQueries = Collections.singletonList(flushStep);
        Void response = null;
        Map<Byte, CString> errorDetails = null;
        if (transferMode == TransferMode.FORWARD) {
            // nothing to do, just forward query
        } else if (transferMode == TransferMode.FORGET) {
            // Buffer extended query
            errorDetails = bufferQuery(ctx, flushStep);
            if (errorDetails != null) {
                transferMode = TransferMode.ERROR;
            }
            newQueries = null;
        } else if (transferMode == TransferMode.ERROR) {
            errorDetails = session.getRetainedTransactionErrorDetails();
            session.resetCurrentCommand();
            newQueries = null;
        }
        QueriesTransferMode<FlushStep, Void> mode = new QueriesTransferMode<>(newQueries, transferMode, response,
                errorDetails);
        LOGGER.debug("Flush step processed: new queries={}, transfer mode={}", mode.getNewQueries(),
                mode.getTransferMode());
        return mode;
    }

    private Result<List<Query>, CommandResults, CString> buildNewQueries(ChannelHandlerContext ctx,
            ExtendedQueryStatus<ParseStep> parseStepStatus, ExtendedQueryStatus<BindStep> bindStepStatus)
            throws IOException {
        List<ExtendedQuery> newExtendedQuery = null;
        if (parseStepStatus.isToProcess()) {
            Result<List<ExtendedQuery>, CommandResults, CString> result = processExtendedQuery(ctx, parseStepStatus,
                    bindStepStatus);
            if (result.isQuery()) {
                newExtendedQuery = result.query();
            } else if (result.isResponse()) {
                return Result.response(result.response());
            } else if (result.isError()) {
                return Result.error(result.error());
            }
        } else {
            newExtendedQuery = Arrays.asList(parseStepStatus.getQuery(), bindStepStatus.getQuery());
        }
        List<Query> newQueries = processBufferedQueries(ctx);
        newExtendedQuery.forEach(Query::retain);
        if (newQueries.isEmpty()) {
            newQueries = newExtendedQuery.stream().map(q -> (Query) q).collect(Collectors.toList());
        } else {
            newQueries.addAll(newExtendedQuery);
        }
        return Result.query(newQueries);
    }

    private Result<List<ExtendedQuery>, CommandResults, CString> processExtendedQuery(ChannelHandlerContext ctx,
            ExtendedQueryStatus<ParseStep> parseStepStatus, ExtendedQueryStatus<BindStep> bindStepStatus) {
        ParseStep parseStep = parseStepStatus.getQuery();
        BindStep bindStep = bindStepStatus.getQuery();
        // Parse SQL statement
        Statement stmt = parseSQL(ctx, parseStep.getSQL());
        SQLSession session = getSession(ctx);
        Result<List<ExtendedQuery>, CommandResults, CString> result = null;
        if (stmt != null) {
            // Build bind parameter (type+format+value)
            List<ParameterValue> parameterValues = bindStep.getParameterValues().stream().map(ParameterValue::new) // create a ParameterValue for each parameter value
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
                if (stmt instanceof CreateTable) {
                    moduleOperation = extractCreateTableOperation(ctx, (CreateTable) stmt);
                } else if (stmt instanceof Insert) {
                    moduleOperation = extractInsertOperation(ctx, (Insert) stmt, parameterValues, null);
                } else if (stmt instanceof Select) {
                    moduleOperation = extractSelectOperation(ctx, (Select) stmt, parameterValues);
                } else if (stmt instanceof Update) {
                    // TODO Update
                } else if (stmt instanceof Delete) {
                    // TODO Delete
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
                // Forget SQL statement, reply directly to the frontend
                CommandResults commandResults = new CommandResults();
                commandResults.setBindCompleteRequired(true);
                result = Result.response(commandResults);
            } else if (moduleOperation instanceof DataOperation) {
                DataOperation dataOperation = (DataOperation) moduleOperation;
                // Save column names (row description)
                List<CString> columns = dataOperation.getDataIds().stream()
                        .map(id -> id.substring(id.lastIndexOf('/') + 1)).collect(Collectors.toList());
                parseStep.setColumns(columns);
                // Process data operation
                List<DataOperation> newDataOperations = getProtocolService(ctx).newDataOperation(dataOperation);
                dataOperation = newDataOperations.get(0);
                if (dataOperation.isModified()) {
                    // Modify SQL statement
                    if (stmt instanceof Insert) {
                        modifyInsertStatement(ctx, (Insert) stmt, parameterValues, dataOperation, 0);
                    } else if (stmt instanceof Select) {
                        modifySelectStatement(ctx, (Select) stmt, parameterValues, dataOperation, 0);
                    }
                    String newSQL = stmt.toString();
                    newSQL = StringUtilities.addIrrelevantCharacters(newSQL, parseStep.getSQL(), " \t\r\n;");
                    parseStep = new ParseStep(parseStep.getName(), CString.valueOf(newSQL), parseStep.isMetadata(),
                            parseStep.getColumns(), parseStep.getParameterTypes());
                    boolean allMatch = parameterValues.stream().map(ParameterValue::getValue)
                            .collect(Collectors.toList()) // build a list of parameter ByteBuf values
                            .equals(bindStep.getParameterValues()); // test if parameter ByteBuf values has been changed
                    if (!allMatch) {
                        List<ByteBuf> newParameterValues = parameterValues.stream().map(ParameterValue::getValue) // get the parameter value
                                .collect(Collectors.toList()); // build a list
                        bindStep = new BindStep(bindStep.getName(), bindStep.getPreparedStatement(),
                                bindStep.getParameterFormats(), newParameterValues, bindStep.getResultColumnFormats());
                    }
                }
                if (parseStepStatus.getOperation() == Operation.READ) {
                    session.setPromise(dataOperation.getPromise());
                    session.setResultProcessingEnabled(dataOperation.isResultProcessingEnabled());
                    session.setInvolvedCSPs(dataOperation.getInvolvedCSPs());
                    session.setDataIds(dataOperation.getDataIds());
                }
            }
        }
        if (result == null) {
            List<ExtendedQuery> newQueries = new ArrayList<ExtendedQuery>();
            if (!parseStepStatus.isProcessed()) {
                newQueries.add(parseStep);
                parseStepStatus.setProcessed(true);
            }
            DescribeStep describeStep = session.getDescribeStep((byte) 'S', parseStep.getName());
            if (describeStep != null) {
                newQueries.add(describeStep);
                session.removeDescribeStep(describeStep.getCode(), describeStep.getName());
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
                    // command ignored, don't expect to receive command complete response
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
                        // command ignored, don't expect to receive bind complete response
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
                        // command ignored, don't expect to receive parameter description, row description or no data responses
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
                        // command ignored, don't expect to receive command complete response
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
                        // command ignored, don't expect to receive close complete response
                        // don't notify frontend of error
                    } else {
                        // expect to receive error response
                        session.addLastQueryResponseToIgnore(
                                QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR);
                        // notify frontend of error
                        errorDetails = session.getRetainedTransactionErrorDetails();
                    }
                } else if (extendedQuery instanceof SynchronizeStep) {
                    // expect to receive ready for query response after error response
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
                    if (session.getCurrentCommandOperation() == Operation.READ) {
                        session.addLastQueryResponseToIgnore(QueryResponseType.ROW_DESCRIPTION_AND_ROW_DATA);
                    } else {
                        session.addLastQueryResponseToIgnore(QueryResponseType.NO_DATA);
                    }
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
    public MessageTransferMode<Void> processParseCompleteResponse(ChannelHandlerContext ctx) {
        LOGGER.debug("Parse complete");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        if (nextQueryResponseToIgnore == null) {
            transferMode = TransferMode.FORWARD;
        } else if (nextQueryResponseToIgnore == QueryResponseType.PARSE_COMPLETE) {
            transferMode = TransferMode.FORGET;
            session.removeFirstQueryResponseToIgnore();
        } else {
            throw new IllegalStateException(String.format("Unexpected %s response (%s was expected)",
                    QueryResponseType.PARSE_COMPLETE, nextQueryResponseToIgnore));
        }
        MessageTransferMode<Void> mode = new MessageTransferMode<>(null, transferMode);
        LOGGER.debug("Parse complete processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Void> processBindCompleteResponse(ChannelHandlerContext ctx) {
        LOGGER.debug("Bind complete");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        if (nextQueryResponseToIgnore == null) {
            transferMode = TransferMode.FORWARD;
        } else if (nextQueryResponseToIgnore == QueryResponseType.BIND_COMPLETE) {
            transferMode = TransferMode.FORGET;
            session.removeFirstQueryResponseToIgnore();
        } else {
            throw new IllegalStateException(String.format("Unexpected %s response (%s was expected)",
                    QueryResponseType.BIND_COMPLETE, nextQueryResponseToIgnore));
        }
        MessageTransferMode<Void> mode = new MessageTransferMode<>(null, transferMode);
        LOGGER.debug("Bind complete processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<List<Long>> processParameterDescriptionResponse(ChannelHandlerContext ctx,
            List<Long> types) {
        LOGGER.debug("Parameter description: {}", types);
        TransferMode transferMode;
        List<Long> newTypes = null;
        SQLSession session = getSession(ctx);
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        if (nextQueryResponseToIgnore == null) {
            transferMode = TransferMode.FORWARD;
            newTypes = types;
        } else if (nextQueryResponseToIgnore == QueryResponseType.PARAMETER_DESCRIPTION) {
            transferMode = TransferMode.FORGET;
            session.removeFirstQueryResponseToIgnore();
        } else {
            throw new IllegalStateException(String.format("Unexpected %s response (%s was expected)",
                    QueryResponseType.PARAMETER_DESCRIPTION, nextQueryResponseToIgnore));
        }
        MessageTransferMode<List<Long>> mode = new MessageTransferMode<>(newTypes, transferMode);
        LOGGER.debug("Parameter description processed: new types={}, transfer mode={}", mode.getNewContent(),
                mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>> processRowDescriptionResponse(
            ChannelHandlerContext ctx, List<PgsqlRowDescriptionMessage.Field> fields) {
        LOGGER.debug("Row description: {}", fields);
        TransferMode transferMode;
        List<PgsqlRowDescriptionMessage.Field> newFields = null;
        SQLSession session = getSession(ctx);
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        if (nextQueryResponseToIgnore == null || nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY) {
            transferMode = TransferMode.FORWARD;
            newFields = fields;
            if (session.getCurrentCommandOperation() == Operation.READ) {
                if (!session.isResultProcessingEnabled()) {
                    // Modify field names to be full qualified
                    // Process metadata operation
                    MetadataOperation metadataOperation = new MetadataOperation();
                    metadataOperation.setDataIds(session.getDataIds());
                    metadataOperation = newMetaDataOperation(ctx, metadataOperation);
                    List<Pattern> involvedCSPPatterns = session.getInvolvedCSPs().stream() // for each involved CSP
                            .map(csp -> csp.append("/.*")) // build regex
                            .map(CString::toString) // transform to String
                            .map(Pattern::compile) // compile pattern
                            .collect(Collectors.toList()); // save patterns as list
                    Map<CString, CString> protectedColumns = metadataOperation.getMetadata().values().stream() // for all protected data identifiers
                            .flatMap(List::stream) // flatten the lists
                            .filter(pc -> pc != null) // exclude null protected columns
                            .map(CString::toString) // transform to String
                            .filter(pc -> involvedCSPPatterns.stream().anyMatch(p -> p.matcher(pc).matches())) // filter protected columns with involved CSP patterns
                            .map(pc -> pc.replace('/', '.')) // replace / by .
                            .map(CString::valueOf) // transform to CString
                            .filter(pc -> fields.stream().anyMatch(f -> pc.endsWith(f.getName()))) // filter protected columns that appear in fields
                            .collect(Collectors.toMap(pc -> pc.substring(pc.lastIndexOf('.') + 1),
                                    java.util.function.Function.identity()));
                    newFields = fields.stream().map(f -> {
                        CString protectedColumn = protectedColumns.get(f.getName());
                        if (protectedColumn == null) {
                            protectedColumn = (CString) f.getName().clone();
                        }
                        f.setName(protectedColumn);
                        return f;
                    }).collect(Collectors.toList());
                }
                session.setRowDescription(newFields);
                // Modify promise in case query don't explicitly specify columns (e.g. '*')
                if (session.getPromise() instanceof DefaultPromise && (session.getPromise().getAttributeNames() == null
                        || session.getPromise().getAttributeNames().length == 0)) {
                    ((DefaultPromise) session.getPromise())
                            .setAttributeNames(newFields.stream().map(PgsqlRowDescriptionMessage.Field::getName)
                                    .map(CString::toString).toArray(String[]::new));
                }
            }
        } else if (nextQueryResponseToIgnore == QueryResponseType.ROW_DESCRIPTION_AND_ROW_DATA) {
            transferMode = TransferMode.FORGET;
            session.removeFirstQueryResponseToIgnore();
        } else {
            throw new IllegalStateException(String.format("Unexpected %s response (%s was expected)",
                    QueryResponseType.ROW_DESCRIPTION_AND_ROW_DATA, nextQueryResponseToIgnore));
        }
        MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>> mode = new MessageTransferMode<>(newFields,
                transferMode);
        LOGGER.debug("Row description processed: new fields={}, transfer mode={}", mode.getNewContent(),
                mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<List<ByteBuf>> processDataRowResponse(ChannelHandlerContext ctx, List<ByteBuf> values)
            throws IOException {
        LOGGER.debug("Data row: {}", values);
        TransferMode transferMode;
        List<ByteBuf> newValues = null;
        SQLSession session = getSession(ctx);
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        if (nextQueryResponseToIgnore == null || nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY) {
            transferMode = TransferMode.FORWARD;
            newValues = values;
            if (session.getCurrentCommandOperation() == Operation.READ) {
                newValues = processDataResult(ctx, values);
            }
        } else if (nextQueryResponseToIgnore == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
            transferMode = TransferMode.FORGET;
        } else {
            throw new IllegalStateException(String.format("Unexpected %s response (%s was expected)",
                    QueryResponseType.NO_DATA, nextQueryResponseToIgnore));
        }
        MessageTransferMode<List<ByteBuf>> mode = new MessageTransferMode<>(newValues, transferMode);
        LOGGER.debug("Data row processed: new values={}, transfer mode={}", mode.getNewContent(),
                mode.getTransferMode());
        return mode;
    }

    private List<ByteBuf> processDataResult(ChannelHandlerContext ctx, List<ByteBuf> values) {
        List<ByteBuf> newValues = values;
        SQLSession session = getSession(ctx);
        if (session.isResultProcessingEnabled()) {
            // Extract data operation
            DataOperation dataOperation = new DataOperation();
            dataOperation.setOperation(Operation.READ);
            int i = 0;
            List<CString> dataValues = new ArrayList<>();
            for (PgsqlRowDescriptionMessage.Field field : session.getRowDescription()) {
                dataOperation.addDataId(field.getName());
                CString dataValue = convertToText(field.getTypeOID(), field.getFormat(), values.get(i));
                dataValues.add(dataValue);
                i++;
            }
            dataOperation.addDataValues(dataValues);
            dataOperation.setPromise(session.getPromise());
            // Process data operation
            List<DataOperation> newDataOperations = getProtocolService(ctx).newDataOperation(dataOperation);
            dataOperation = newDataOperations.get(0);
            newValues = values;
            if (dataOperation.isModified()) {
                // Modify data
                for (i = 0; i < values.size(); i++) {
                    CString dataValue = dataValues.get(i);
                    CString newDataValue = dataOperation.getDataValues().get(0).get(i);
                    if (newDataValue != dataValue && (newDataValue == null || !newDataValue.equals(dataValue))) {
                        if (newValues == values) {
                            newValues = new ArrayList<>(values);
                        }
                        newValues.set(i, newDataValue == null ? null : newDataValue.getByteBuf());
                    }
                }
            }
        }
        return newValues;
    }

    private CString convertToText(long type, short format, ByteBuf value) {
        CString cs;
        if (format == 0) {
            // Text format
            cs = value != null ? CString.valueOf(value, value.capacity()) : null;
        } else {
            // TODO Binary format
            throw new UnsupportedOperationException("convert binary to text not yet supported");
        }
        return cs;
    }

    private ByteBuf convertToByteBuf(long type, short format, CString cs) {
        ByteBuf value;
        if (format == 0) {
            // Text format
            value = cs != null ? cs.getByteBuf() : null;
        } else {
            // TODO Binary format
            throw new UnsupportedOperationException("convert text to binary not yet supported");
        }
        return value;
    }

    @Override
    public MessageTransferMode<Void> processNoDataResponse(ChannelHandlerContext ctx) {
        LOGGER.debug("No data");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        if (nextQueryResponseToIgnore == null) {
            transferMode = TransferMode.FORWARD;
        } else if (nextQueryResponseToIgnore == QueryResponseType.NO_DATA) {
            transferMode = TransferMode.FORGET;
            session.removeFirstQueryResponseToIgnore();
        } else {
            throw new IllegalStateException(String.format("Unexpected %s response (%s was expected)",
                    QueryResponseType.NO_DATA, nextQueryResponseToIgnore));
        }
        MessageTransferMode<Void> mode = new MessageTransferMode<>(null, transferMode);
        LOGGER.debug("No data processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<CString> processCommandCompleteResult(ChannelHandlerContext ctx, CString tag)
            throws IOException {
        LOGGER.debug("Command complete: {}", tag);
        TransferMode transferMode;
        CString newTag = null;
        SQLSession session = getSession(ctx);
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        if (nextQueryResponseToIgnore == null || nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY) {
            transferMode = TransferMode.FORWARD;
            newTag = tag;
            session.resetCurrentCommand();
        } else if (nextQueryResponseToIgnore == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
            transferMode = TransferMode.FORGET;
            session.removeFirstQueryResponseToIgnore();
        } else {
            throw new IllegalStateException(String.format("Unexpected %s response (%s was expected)",
                    QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR,
                    nextQueryResponseToIgnore));
        }
        MessageTransferMode<CString> mode = new MessageTransferMode<>(newTag, transferMode);
        LOGGER.debug("Command complete processed: new tag={}, transfer mode={}", mode.getNewContent(),
                mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Void> processEmptyQueryResponse(ChannelHandlerContext ctx) throws IOException {
        LOGGER.debug("Empty query");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        if (nextQueryResponseToIgnore == null || nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY) {
            transferMode = TransferMode.FORWARD;
            session.resetCurrentCommand();
        } else if (nextQueryResponseToIgnore == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
            transferMode = TransferMode.FORGET;
            session.removeFirstQueryResponseToIgnore();
        } else {
            throw new IllegalStateException(String.format("Unexpected %s response (%s was expected)",
                    QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR,
                    nextQueryResponseToIgnore));
        }
        MessageTransferMode<Void> mode = new MessageTransferMode<>(null, transferMode);
        LOGGER.debug("Empty query processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Void> processPortalSuspendedResponse(ChannelHandlerContext ctx) throws IOException {
        LOGGER.debug("Portal suspended");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        if (nextQueryResponseToIgnore == null || nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY) {
            transferMode = TransferMode.FORWARD;
        } else if (nextQueryResponseToIgnore == QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR) {
            transferMode = TransferMode.FORGET;
            session.removeFirstQueryResponseToIgnore();
        } else {
            throw new IllegalStateException(String.format("Unexpected %s response (%s was expected)",
                    QueryResponseType.COMMAND_COMPLETE_OR_EMPTY_QUERY_OR_PORTAL_SUSPENDED_OR_ERROR,
                    nextQueryResponseToIgnore));
        }
        MessageTransferMode<Void> mode = new MessageTransferMode<>(null, transferMode);
        LOGGER.debug("Portal suspended processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Map<Byte, CString>> processErrorResult(ChannelHandlerContext ctx,
            Map<Byte, CString> fields) throws IOException {
        LOGGER.debug("Error: {}", fields);
        TransferMode transferMode;
        Map<Byte, CString> newFields = null;
        // Save current error
        SQLSession session = getSession(ctx);
        session.setTransactionErrorDetails(fields);
        // Skip any expected response before expected ready for query
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        while (nextQueryResponseToIgnore != null && nextQueryResponseToIgnore != QueryResponseType.READY_FOR_QUERY) {
            session.removeFirstQueryResponseToIgnore();
            nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        }
        if (session.getQueryResponsesToIgnore().isEmpty()) {
            transferMode = TransferMode.FORWARD;
            newFields = fields;
            session.resetCurrentCommand();
        } else {
            transferMode = TransferMode.FORGET;
        }
        MessageTransferMode<Map<Byte, CString>> mode = new MessageTransferMode<>(newFields, transferMode);
        LOGGER.debug("Error processed: new fields={}, transfer mode={}", mode.getNewContent(), mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Void> processCloseCompleteResponse(ChannelHandlerContext ctx) {
        LOGGER.debug("Close complete");
        TransferMode transferMode;
        SQLSession session = getSession(ctx);
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        if (nextQueryResponseToIgnore == null) {
            transferMode = TransferMode.FORWARD;
        } else if (nextQueryResponseToIgnore == QueryResponseType.CLOSE_COMPLETE) {
            transferMode = TransferMode.FORGET;
            session.removeFirstQueryResponseToIgnore();
        } else {
            throw new IllegalStateException(String.format("Unexpected %s response (%s was expected)",
                    QueryResponseType.CLOSE_COMPLETE, nextQueryResponseToIgnore));
        }
        MessageTransferMode<Void> mode = new MessageTransferMode<>(null, transferMode);
        LOGGER.debug("Close complete processed: transfer mode={}", mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Byte> processReadyForQueryResponse(ChannelHandlerContext ctx, Byte transactionStatus)
            throws IOException {
        LOGGER.debug("Ready for query: {}", (char) transactionStatus.byteValue());
        TransferMode transferMode;
        Byte newTransactionStatus = null;
        // Save current transaction status
        SQLSession session = getSession(ctx);
        session.setTransactionStatus(transactionStatus);
        if (transactionStatus != (byte) 'E') {
            // Reset current error
            session.setTransactionErrorDetails(null);
        }
        QueryResponseType nextQueryResponseToIgnore = session.firstQueryResponseToIgnore();
        if (nextQueryResponseToIgnore == null) {
            transferMode = TransferMode.FORWARD;
            newTransactionStatus = transactionStatus;
        } else if (nextQueryResponseToIgnore == QueryResponseType.READY_FOR_QUERY) {
            transferMode = TransferMode.FORGET;
            session.removeFirstQueryResponseToIgnore();
        } else {
            throw new IllegalStateException(String.format("Unexpected %s response (%s was expected)",
                    QueryResponseType.READY_FOR_QUERY, nextQueryResponseToIgnore));
        }
        MessageTransferMode<Byte> mode = new MessageTransferMode<>(newTransactionStatus, transferMode);
        LOGGER.debug("Ready for query processed: new transaction status={}, transfer mode={}",
                newTransactionStatus == null ? null : (char) newTransactionStatus.byteValue(), mode.getTransferMode());
        return mode;
    }

    private SQLSession getSession(ChannelHandlerContext ctx) {
        PgsqlSession pgsqlSession = (PgsqlSession) ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        return pgsqlSession.getSqlSession();
    }

    private Mode getProcessingMode(ChannelHandlerContext ctx, boolean wholeDataset, Operation operation) {
        Configuration configuration = ctx.channel().attr(PgsqlConstants.CONFIGURATION_KEY).get();
        return configuration.getProcessingMode(wholeDataset, operation);
    }

    private ProtocolService getProtocolService(ChannelHandlerContext ctx) {
        Configuration configuration = ctx.channel().attr(PgsqlConstants.CONFIGURATION_KEY).get();
        return configuration.getProtocolService();
    }

}
