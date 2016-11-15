package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConstants;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage.Field;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SqlSession.BufferedStatement;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.Mode;
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
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.TokenMgrError;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class PgsqlEventProcessor implements EventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);

    public static final String USER_KEY = "user";
    public static final String DATABASE_KEY = "database";

    @Override
    public CString processAuthentication(ChannelHandlerContext ctx, Map<CString, CString> parameters) throws IOException {
        CString databaseName = parameters.get(CString.valueOf(DATABASE_KEY));
        SqlSession sqlSession = getSession(ctx);
        sqlSession.setDatabaseName((CString) databaseName.clone());
        CString userName = getProtocolService(ctx).newUserIdentification(parameters.get(CString.valueOf(USER_KEY)));
        return userName;
    }

    @Override
    public StatementTransferMode processStatement(ChannelHandlerContext ctx, CString statement, boolean lastStatement) throws IOException {
        LOGGER.debug("SQL statement: {}", statement);
        TransferMode transferMode;
        CString newStatements = statement;
        CString response = null;
        if (statement.isEmpty()) {
            transferMode = getSession(ctx).getTransferMode();
        } else {
            transferMode = TransferMode.FORWARD;
            boolean toProcess = false;
            Mode processingMode;
            StatementType type = SimpleSqlParserUtil.parse(statement);
            if (type != null) {
                switch (type) {
                case START_TRANSACTION:
                    getSession(ctx).setInTransaction(true);
                    break;
                case COMMIT:
                case ROLLBACK:
                    getSession(ctx).setInTransaction(false);
                    getSession(ctx).setInDatasetCreation(false);
                    break;
                case SELECT:
                    processingMode = getProcessingMode(ctx, false, Operation.READ);
                    toProcess = isStatementToProcess(processingMode);
                    if (processingMode == null) {
                        transferMode = TransferMode.DENY;
                        response = CString.valueOf("Record read not supported by this CLARUS proxy");
                    }
                    if (toProcess) {
                        getSession(ctx).setCurrentOperation(Operation.READ);
                    }
                    break;
                case INSERT:
                    if (getSession(ctx).isInDatasetCreation()) {
                        processingMode = getProcessingMode(ctx, true, Operation.CREATE);
                        toProcess = isStatementToProcess(processingMode);
                        if (processingMode == null) {
                            transferMode = TransferMode.DENY;
                            response = CString.valueOf("Dataset creation not supported by this CLARUS proxy");
                        } else if (processingMode == Mode.BUFFERING) {
                            transferMode = TransferMode.FORGET;
                            if (lastStatement) {
                                response = CString.valueOf("INSERT 0 1");
                            }
                        } else if (toProcess) {
                            getSession(ctx).setCurrentOperation(Operation.CREATE);
                        }
                    } else {
                        processingMode = getProcessingMode(ctx, false, Operation.CREATE);
                        toProcess = isStatementToProcess(processingMode);
                        if (processingMode == null) {
                            transferMode = TransferMode.DENY;
                            response = CString.valueOf("Record creation not supported by this CLARUS proxy");
                        } else if (toProcess) {
                            getSession(ctx).setCurrentOperation(Operation.CREATE);
                        }
                    }
                    break;
                case CREATE_TABLE:
                    if (getProcessingMode(ctx, true, Operation.CREATE) == null) {
                        transferMode = TransferMode.DENY;
                        response = CString.valueOf("Dataset creation not supported by this CLARUS proxy");
                    }
                    if (getSession(ctx).isInTransaction()) {
                        getSession(ctx).setInDatasetCreation(true);
                    }
                    break;
                case ADD_GEOMETRY_COLUMN:
                    toProcess = true;
                default:
                    break;
                }
            }
            getSession(ctx).setTransferMode(transferMode);
            if (transferMode == TransferMode.FORWARD) {
                newStatements = buildNewStatements(ctx, statement, toProcess, lastStatement);
            } else if (transferMode == TransferMode.FORGET) {
                if (statement != null) {
                    bufferStatement(ctx, statement, toProcess, lastStatement);
                }
                newStatements = null;
            } else if (transferMode == TransferMode.DENY) {
                getSession(ctx).resetCurrentCommand();
                newStatements = null;
            }
        }
        StatementTransferMode mode = new StatementTransferMode(newStatements, transferMode, response);
        LOGGER.debug("SQL statement processed: new statements={}, transfer mode={}", mode.getNewStatements(), mode.getTransferMode());
        return mode;
    }

    private boolean isStatementToProcess(Mode processingMode) {
        String statementForceProcessing = System.getProperty("pgsql.statement.force.processing");
        if (statementForceProcessing != null && (Boolean.TRUE.toString().equalsIgnoreCase(statementForceProcessing) || "1".equalsIgnoreCase(statementForceProcessing) || "yes".equalsIgnoreCase(statementForceProcessing) || "on".equalsIgnoreCase(statementForceProcessing))) {
            return true;
        }
        return processingMode != null && processingMode != Mode.AS_IT_IS;
    }

    private CString buildNewStatements(ChannelHandlerContext ctx, CString statement, boolean toProcess, boolean lastStatement) throws IOException {
        processBufferedStatements(ctx);
        CString newStatement = toProcess ? processStatement(ctx, statement) : statement;
        CString newStatements;
        if (getSession(ctx).getBufferedStatements().isEmpty()) {
            newStatements = newStatement;
        } else {
            // Compute new statement size
            int newSize = 0;
            for (BufferedStatement bufferedStatement : getSession(ctx).getBufferedStatements()) {
                newSize += bufferedStatement.getResult().length();
            }
            newSize += newStatement.length();
            newStatements = CString.valueOf(new StringBuilder(newSize));
            for (Iterator<BufferedStatement> iter = getSession(ctx).getBufferedStatements().iterator(); iter.hasNext();) {
                BufferedStatement bufferedStatement = iter.next();
                newStatements.append(bufferedStatement.getResult());
                iter.remove();
            }
            newStatements.append(newStatement);
        }
        return newStatements;
    }

    private void processBufferedStatements(ChannelHandlerContext ctx) {
        DataOperation dataOperation = null;
        for (BufferedStatement bufferedStatement : getSession(ctx).getBufferedStatements()) {
            if (!bufferedStatement.isToProcess()) {
                continue;
            }
            // Parse statement
            Statement stmt = parseStatement(ctx, bufferedStatement.getOriginal());
            if (stmt == null) {
                continue;
            }
            // Extract data operation
            if (stmt instanceof Insert) {
                dataOperation = extractInsertOperation(ctx, (Insert)stmt, dataOperation);
            } else if (stmt instanceof Select) {
                dataOperation = extractSelectOperation(ctx, (Select)stmt, dataOperation);
            }
            bufferedStatement.setStmt(stmt);
        }
        // Process data operation
        if (dataOperation != null) {
            List<DataOperation> newDataOperations = getProtocolService(ctx).newDataOperation(dataOperation);
            dataOperation = newDataOperations.get(0);
            if (dataOperation.isModified()) {
                // Modify statements
                int row = 0;
                for (BufferedStatement bufferedStatement : getSession(ctx).getBufferedStatements()) {
                    if (!bufferedStatement.isToProcess()) {
                        continue;
                    }
                    // Modify statement
                    if (bufferedStatement.getStmt() instanceof Insert) {
                        modifyInsertOperation(ctx, (Insert)bufferedStatement.getStmt(), dataOperation, row ++);
                    } else if (bufferedStatement.getStmt() instanceof Select) {
                        modifySelectOperation(ctx, (Select)bufferedStatement.getStmt(), dataOperation, row ++);
                    }
                    String newStatement = bufferedStatement.getStmt().toString();
                    newStatement = StringUtilities.addIrrelevantCharacters(newStatement, bufferedStatement.getOriginal(), " \t\r\n;");
                    bufferedStatement.setModified(CString.valueOf(newStatement));
                }
            }
        }
    }

    private CString processStatement(ChannelHandlerContext ctx, CString statement) {
        // Parse statement
        Statement stmt = parseStatement(ctx, statement);
        if (stmt == null) {
            return statement;
        }
        // Extract data operation
        DataOperation dataOperation = null;
        if (stmt instanceof Insert) {
            dataOperation = extractInsertOperation(ctx, (Insert)stmt, null);
        } else if (stmt instanceof Select) {
            dataOperation = extractSelectOperation(ctx, (Select)stmt, null);
        }
        // Process data operation
        if (dataOperation != null) {
            List<DataOperation> newDataOperations = getProtocolService(ctx).newDataOperation(dataOperation);
            dataOperation = newDataOperations.get(0);
            if (dataOperation.isModified()) {
                // Modify statement
                if (stmt instanceof Insert) {
                    modifyInsertOperation(ctx, (Insert)stmt, dataOperation, 0);
                } else if (stmt instanceof Select) {
                    modifySelectOperation(ctx, (Select)stmt, dataOperation, 0);
                }
                String newStatement = stmt.toString();
                newStatement = StringUtilities.addIrrelevantCharacters(newStatement, statement, " \t\r\n;");
                statement = CString.valueOf(newStatement);
            }
            if (getSession(ctx).getCurrentOperation() == Operation.READ) {
                getSession(ctx).setPromise(dataOperation.getPromise());
            }
        }
        return statement;
    }

    private Statement parseStatement(ChannelHandlerContext ctx, CString statement) {
        // Parse statement
        Statement stmt = null;
        ByteBuf byteBuf = null;
        try {
            if (statement.isBuffered()) {
                byteBuf = statement.getByteBuf();
                byteBuf.markReaderIndex();
                stmt = CCJSqlParserUtil.parse(new ByteBufInputStream(byteBuf.readSlice(statement.length())), StandardCharsets.ISO_8859_1.name());
            } else {
                stmt = CCJSqlParserUtil.parse(statement.toString());
            }
        } catch (JSQLParserException | TokenMgrError e) {
            if (byteBuf != null) {
                byteBuf.resetReaderIndex();
            }
            LOGGER.error("Parsing error for {} : ", statement, e);
        }
        return stmt;
    }

    private DataOperation extractInsertOperation(ChannelHandlerContext ctx, Insert stmt, DataOperation dataOperation) {
        if (dataOperation == null) {
            dataOperation = new DataOperation();
            dataOperation.setOperation(Operation.CREATE);
            // Extract dataset id
            StringBuilder sb = new StringBuilder();
            if (stmt.getTable().getDatabase() != null) {
                String databaseName = StringUtilities.unquote(stmt.getTable().getDatabase().getDatabaseName());
                if (databaseName != null && !databaseName.isEmpty()) {
                    sb.append(databaseName).append('/');
                }
            }
            if (sb.length() == 0 && getSession(ctx).getDatabaseName() != null) {
                sb.append(getSession(ctx).getDatabaseName()).append('/');
            }
            String schemaName = StringUtilities.unquote(stmt.getTable().getSchemaName());
            if (schemaName != null) {
                sb.append(schemaName).append('.');
            }
            String tableName = StringUtilities.unquote(stmt.getTable().getName());
            sb.append(tableName).append('/');
            String datasetId = sb.toString();
            // Extract data ids
            for (Column column : stmt.getColumns()) {
                dataOperation.addDataId(CString.valueOf(datasetId + StringUtilities.unquote(column.getColumnName())));
            }
        }
        // Extract data values
        List<CString> dataValues = new ArrayList<>();
        for (Expression expression : ((ExpressionList) stmt.getItemsList()).getExpressions()) {
            dataValues.add(expression instanceof NullValue ? null : CString.valueOf(expression.toString()));
        }
        dataOperation.addDataValues(dataValues);
        return dataOperation;
    }

    private void modifyInsertOperation(ChannelHandlerContext ctx, Insert stmt, DataOperation dataOperation, int row) {
        List<CString> dataValues = dataOperation.getDataValues().get(row);
        List<Expression> values = ((ExpressionList) stmt.getItemsList()).getExpressions();
        for (int i = 0; i < values.size(); i ++) {
            CString dataValue = dataValues.get(i);
            values.set(i, dataValue == null ? new NullValue() : new StringValue(dataValue.toString()));
        }
    }

    private DataOperation extractSelectOperation(ChannelHandlerContext ctx, Select stmt, DataOperation dataOperation) {
        if (dataOperation == null) {
            dataOperation = new DataOperation();
            dataOperation.setOperation(Operation.READ);
            if (!(stmt.getSelectBody() instanceof PlainSelect)) {
                return null;
            }
            // Extract dataset id
            String datasetId;
            PlainSelect select = (PlainSelect) stmt.getSelectBody();
            if (select.getFromItem() instanceof Table) {
                StringBuilder sb = new StringBuilder();
                Table table = (Table) select.getFromItem();
                if (table.getDatabase() != null) {
                    String databaseName = StringUtilities.unquote(table.getDatabase().getDatabaseName());
                    if (databaseName != null && !databaseName.isEmpty()) {
                        sb.append(databaseName).append('/');
                    }
                }
                if (sb.length() == 0 && getSession(ctx).getDatabaseName() != null) {
                    sb.append(getSession(ctx).getDatabaseName()).append('/');
                }
                String schemaName = StringUtilities.unquote(table.getSchemaName());
                if (schemaName != null) {
                    sb.append(schemaName).append('.');
                }
                String tableName = StringUtilities.unquote(table.getName());
                sb.append(tableName).append('/');
                datasetId = sb.toString();
            } else {
                datasetId = "";
            }
            // Extract data ids
            for (SelectItem selectItem : select.getSelectItems()) {
                if (selectItem instanceof SelectExpressionItem) {
                    SelectExpressionItem selectExpressionItem = (SelectExpressionItem)selectItem;
                    if (selectExpressionItem.getExpression() instanceof Function) {
                        dataOperation.addDataId(CString.valueOf(datasetId + ((Function)selectExpressionItem.getExpression()).getName()));
                    } else {
                        dataOperation.addDataId(CString.valueOf(datasetId + StringUtilities.unquote(selectExpressionItem.getExpression().toString())));
                    }
                } /* else TODO All columns (*) */
            }
            // Extract parameter ids and values (functions)
            for (SelectItem selectItem : select.getSelectItems()) {
                if (selectItem instanceof SelectExpressionItem && ((SelectExpressionItem)selectItem).getExpression() instanceof Function) {
                    Function function = (Function) ((SelectExpressionItem)selectItem).getExpression();
                    if (function.getParameters() != null) {
                        dataOperation.addParameterId(CString.valueOf(function.getName()));
                        dataOperation.addParameterValue(CString.valueOf(PlainSelect.getStringList(function.getParameters().getExpressions(), true, false)));
                    }
                }
            }
            // TODO Extract parameter ids and values (clause where)
        }
        return dataOperation;
    }

    private void modifySelectOperation(ChannelHandlerContext ctx, Select stmt, DataOperation dataOperation, int row) {
        PlainSelect select = (PlainSelect) stmt.getSelectBody();
        for (SelectItem selectItem : select.getSelectItems()) {
            if (selectItem instanceof SelectExpressionItem && ((SelectExpressionItem)selectItem).getExpression() instanceof Function) {
                Function function = (Function) ((SelectExpressionItem)selectItem).getExpression();
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

    private void bufferStatement(ChannelHandlerContext ctx, CString statement, boolean toProcess, boolean lastStatement) {
        if (getSession(ctx).getBufferedStatements().isEmpty()) {
            getSession(ctx).resetCommandResultsToIgnore();
        }
        getSession(ctx).addBufferedStatements(statement, toProcess);
        if (lastStatement) {
            getSession(ctx).incrementeCommandResultsToIgnore();
        }
    }

    @Override
    public MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>> processRowDescriptionResponse(ChannelHandlerContext ctx, List<PgsqlRowDescriptionMessage.Field> fields) {
        LOGGER.debug("Row description: {}", fields);
        TransferMode transferMode = TransferMode.FORWARD;
        List<PgsqlRowDescriptionMessage.Field> newFields = fields;
        if (getSession(ctx).getCurrentOperation() == Operation.READ) {
            getSession(ctx).setRowDescription(fields);
            // Modify promise in case query don't explicitly specify columns (e.g. '*')
            if (getSession(ctx).getPromise() instanceof DefaultPromise && (getSession(ctx).getPromise().getAttributeNames() == null || getSession(ctx).getPromise().getAttributeNames().length == 0)) {
                ((DefaultPromise)getSession(ctx).getPromise()).setAttributeNames(newFields.stream().map(PgsqlRowDescriptionMessage.Field::getName).map(CString::toString).toArray(String[]::new));
            }
        }
        MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>> mode = new MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>>(newFields, transferMode);
        LOGGER.debug("Row description processed: new fields={}, transfer mode={}", mode.getNewContent(), mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<List<ByteBuf>> processDataRowResponse(ChannelHandlerContext ctx, List<ByteBuf> values) throws IOException {
        LOGGER.debug("Data row: {}", values);
        TransferMode transferMode = TransferMode.FORWARD;
        List<ByteBuf> newValues = values;
        if (getSession(ctx).getCurrentOperation() == Operation.READ) {
            newValues = processDataResult(ctx, values);
        }
        MessageTransferMode<List<ByteBuf>> mode = new MessageTransferMode<List<ByteBuf>>(newValues, transferMode);
        LOGGER.debug("Data row processed: new values={}, transfer mode={}", mode.getNewContent(), mode.getTransferMode());
        return mode;
    }

    private List<ByteBuf> processDataResult(ChannelHandlerContext ctx, List<ByteBuf> values) {
        // Extract data operation
        DataOperation dataOperation = new DataOperation();
        dataOperation.setOperation(Operation.READ);
        int i = 0;
        List<CString> dataValues = new ArrayList<>();
        for (PgsqlRowDescriptionMessage.Field field : getSession(ctx).getRowDescription()) {
            dataOperation.addDataId(field.getName());
            CString dataValue = convert(field, values.get(i));
            dataValues.add(dataValue);
            i ++;
        }
        dataOperation.addDataValues(dataValues);
        dataOperation.setPromise(getSession(ctx).getPromise());
        // Process data operation
        List<DataOperation> newDataOperations = getProtocolService(ctx).newDataOperation(dataOperation);
        dataOperation = newDataOperations.get(0);
        List<ByteBuf> newValues = values;
        if (dataOperation.isModified()) {
            // Modify data
            for (i = 0; i < values.size(); i ++) {
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
        return newValues;
    }

    private CString convert(Field field, ByteBuf value) {
        CString cs;
        if (field.getFormat() == 0) { // Text format
            cs = value != null ? CString.valueOf(value, value.capacity()) : null;
        } else { // Binary format
            // TODO
            cs = CString.valueOf("binary data");
        }
        return cs;
    }

    @Override
    public MessageTransferMode<CString> processCommandCompleteResult(ChannelHandlerContext ctx, CString tag) throws IOException {
        LOGGER.debug("Command complete: {}", tag);
        TransferMode transferMode;
        CString newTag = tag;
        if (getSession(ctx).getCommandResultsToIgnore() == 0) {
            transferMode = TransferMode.FORWARD;
        } else {
            transferMode = TransferMode.FORGET;
            newTag = null;
            getSession(ctx).decrementeCommandResultsToIgnore();
        }
        getSession(ctx).resetCurrentOperation();
        MessageTransferMode<CString> mode = new MessageTransferMode<CString>(newTag, transferMode);
        LOGGER.debug("Command complete processed: new tag={}, transfer mode={}", mode.getNewContent(), mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Map<Byte, CString>> processErrorResult(ChannelHandlerContext ctx, Map<Byte, CString> fields) throws IOException {
        LOGGER.debug("Error: {}", fields);
        TransferMode transferMode = TransferMode.FORWARD;
        Map<Byte, CString> newFields = fields;
        getSession(ctx).resetCurrentOperation();
        MessageTransferMode<Map<Byte, CString>> mode = new MessageTransferMode<Map<Byte, CString>>(newFields, transferMode);
        LOGGER.debug("Command complete processed: new fields={}, transfer mode={}", mode.getNewContent(), mode.getTransferMode());
        return mode;
    }

    @Override
    public MessageTransferMode<Byte> processReadyForQueryResponse(ChannelHandlerContext ctx, Byte transactionStatus) throws IOException {
        LOGGER.debug("Ready for query: {}", (char) transactionStatus.byteValue());
        TransferMode transferMode;
        Byte newTransactionStatus = transactionStatus;
        if (getSession(ctx).getReadyForQueryToIgnore() == 0) {
            transferMode = TransferMode.FORWARD;
        } else {
            transferMode = TransferMode.FORGET;
            newTransactionStatus = null;
            getSession(ctx).decrementeReadyForQueryToIgnore();
        }
        MessageTransferMode<Byte> mode = new MessageTransferMode<Byte>(newTransactionStatus, transferMode);
        LOGGER.debug("Ready for query processed: new transaction status={}, transfer mode={}", newTransactionStatus == null ? null : (char) newTransactionStatus.byteValue(), mode.getTransferMode());
        return mode;
    }

    private SqlSession getSession(ChannelHandlerContext ctx) {
        PgsqlSession pgsqlSession = (PgsqlSession) ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        return pgsqlSession.getSession();
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
