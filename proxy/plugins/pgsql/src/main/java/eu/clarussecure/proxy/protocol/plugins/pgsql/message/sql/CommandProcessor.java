package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConfiguration;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConstants;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlErrorMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SqlSession.BufferedStatement;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.StringUtilities;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
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

public class CommandProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandProcessor.class);

    public StatementTransferMode processStatement(ChannelHandlerContext ctx, CString statement, boolean lastStatement, boolean streaming) throws IOException {
        LOGGER.debug("SQL statement: {}", statement);
        TransferMode transferMode;
        CString newStatements = statement;
        CString response = null;
        if (statement.isEmpty()) {
            transferMode = getSession(ctx).getTransferMode();
        } else {
            transferMode = TransferMode.FORWARD;
            boolean toProcess = false;
            StatementType type = SimpleSqlParserUtil.parse(statement);
            if (type != null) {
                Mode datasetCreationProcessingMode;
                switch (type) {
                case START_TRANSACTION:
                    getSession(ctx).setInTransaction(true);
                    break;
                case COMMIT:
                case ROLLBACK:
                    getSession(ctx).setInTransaction(false);
                    getSession(ctx).setInDatasetCreation(false);
                    break;
                case CREATE_TABLE:
                    datasetCreationProcessingMode = getProcessingMode(ctx, true, Operation.CREATE);
                    if (datasetCreationProcessingMode == null) {
                        transferMode = TransferMode.DENY;
                        response = CString.valueOf("Dataset creation not supported by this CLARUS proxy");
                    }
                    if (getSession(ctx).isInTransaction()) {
                        getSession(ctx).setInDatasetCreation(true);
                    }
                    break;
                case INSERT:
                    toProcess = true;
                    if (getSession(ctx).isInDatasetCreation()) {
                        datasetCreationProcessingMode = getProcessingMode(ctx, true, Operation.CREATE);
                        if (datasetCreationProcessingMode != null && datasetCreationProcessingMode == Mode.BUFFERING) {
                            transferMode = TransferMode.FORGET;
                            if (lastStatement) {
                                response = CString.valueOf("INSERT 0 1");
                            }
                        }
                    } else {
                        datasetCreationProcessingMode = getProcessingMode(ctx, false, Operation.CREATE);
                        if (datasetCreationProcessingMode == null) {
                            transferMode = TransferMode.DENY;
                            response = CString.valueOf("Record creation not supported by this CLARUS proxy");
                        }
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
                newStatements = buildNewStatements(ctx, statement, toProcess, lastStatement, streaming);
            } else if (transferMode == TransferMode.FORGET) {
                if (statement != null) {
                    bufferStatement(ctx, statement, toProcess, lastStatement);
                }
                newStatements = null;
            } else if (transferMode == TransferMode.DENY) {
                resetQueryStatus(ctx);
                newStatements = null;
            }
        }
        StatementTransferMode mode = new StatementTransferMode(newStatements, transferMode, response);
        LOGGER.debug("SQL statement processed: new statements={}, transfer mode={}", mode.getNewStatements(), mode.getTransferMode());
        return mode;
    }

    private CString buildNewStatements(ChannelHandlerContext ctx, CString statement, boolean toProcess, boolean lastStatement, boolean streaming) {
        processBufferedStatements(ctx);
        statement = processStatement(ctx, statement, toProcess);
        if (streaming && !lastStatement) {
            getSession(ctx).incrementeReadyForQueryToIgnore();
        }
        CString newStatements;
        if (getSession(ctx).getBufferedStatements().isEmpty()) {
            newStatements = statement;
        } else {
            newStatements = CString.valueOf("");
            for (BufferedStatement bufferedStatement : getSession(ctx).getBufferedStatements()) {
                newStatements.append(bufferedStatement.getResult(), ctx.alloc());
            }
            getSession(ctx).resetBufferedStatements();
            newStatements.append(statement, ctx.alloc());
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

    private CString processStatement(ChannelHandlerContext ctx, CString statement, boolean toProcess) {
        if (!toProcess) {
            return statement;
        }
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
        return statement;
    }

    private Statement parseStatement(ChannelHandlerContext ctx, CString statement) {
        // Parse statement
        Statement stmt = null;
        try {
            if (statement.isBuffered()) {
                ByteBuf byteBuf = statement.getByteBuf(ctx.alloc()).readSlice(statement.length());
                stmt = CCJSqlParserUtil.parse(new ByteBufInputStream(byteBuf), StandardCharsets.ISO_8859_1.name());
            } else {
                stmt = CCJSqlParserUtil.parse(statement.toString());
            }
        } catch (JSQLParserException | TokenMgrError e) {
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
            dataValues.add(CString.valueOf(expression.toString()));
        }
        dataOperation.addDataValues(dataValues);
        return dataOperation;
    }

    private void modifyInsertOperation(ChannelHandlerContext ctx, Insert stmt, DataOperation dataOperation, int row) {
        List<CString> dataValues = dataOperation.getDataValues().get(row);
        List<Expression> values = ((ExpressionList) stmt.getItemsList()).getExpressions();
        for (int i = 0; i < values.size(); i ++) {
            CString dataValue = dataValues.get(i);
            values.set(i, new StringValue(dataValue.toString()));
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

    private void resetQueryStatus(ChannelHandlerContext ctx) {
        getSession(ctx).resetBufferedStatements();
        getSession(ctx).resetCommandResultsToIgnore();
        getSession(ctx).resetReadyForQueryToIgnore();
    }

    public CommandResultTransferMode processCommandResult(ChannelHandlerContext ctx, CString details) throws IOException {
        LOGGER.debug("Command result: {}", details);
        TransferMode transferMode;
        CString newDetails = details;
        if (getSession(ctx).getCommandResultsToIgnore() == 0) {
            transferMode = TransferMode.FORWARD;
        } else {
            if (PgsqlErrorMessage.isErrorFields(details)) {
                transferMode = TransferMode.FORWARD;
                getSession(ctx).resetCommandResultsToIgnore();
            } else {
                transferMode = TransferMode.FORGET;
                newDetails = null;
                getSession(ctx).decrementeCommandResultsToIgnore();
            }
        }
        CommandResultTransferMode mode = new CommandResultTransferMode(newDetails, transferMode);
        LOGGER.debug("Command result processed: new tag={}, transfer mode={}", mode.getNewDetails(), mode.getTransferMode());
        return mode;
    }

    public ReadyForQueryResponseTransferMode processReadyForQueryResponse(ChannelHandlerContext ctx, Byte transactionStatus) {
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
        ReadyForQueryResponseTransferMode mode = new ReadyForQueryResponseTransferMode(newTransactionStatus, transferMode);
        LOGGER.debug("Ready for query processed: new transaction status={}, transfer mode={}", newTransactionStatus == null ? null : (char) newTransactionStatus.byteValue(), mode.getTransferMode());
        return mode;
    }

    private SqlSession getSession(ChannelHandlerContext ctx) {
        PgsqlSession pgsqlSession = ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        return pgsqlSession.getSession();
    }

    private Mode getProcessingMode(ChannelHandlerContext ctx, boolean wholeDataset, Operation operation) {
        PgsqlConfiguration configuration = ctx.channel().attr(PgsqlConstants.CONFIGURATION_KEY).get();
        return configuration.getProcessingMode(wholeDataset, operation);
    }

    private ProtocolService getProtocolService(ChannelHandlerContext ctx) {
        PgsqlConfiguration configuration = ctx.channel().attr(PgsqlConstants.CONFIGURATION_KEY).get();
        return configuration.getProtocolService();
    }

}
