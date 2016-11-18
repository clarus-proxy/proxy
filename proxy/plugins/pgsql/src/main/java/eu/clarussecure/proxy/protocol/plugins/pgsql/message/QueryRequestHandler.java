package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage.Field;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.converter.PgsqlMessageToQueryConverter;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.QueriesTransferMode;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.Query;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession.QueryResponseType;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLStatement;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SimpleSQLStatement;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer.PgsqlMessageWriter;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.DefaultLastPgsqlRawContent;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawContent;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.buffer.MutableByteBufInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class QueryRequestHandler extends PgsqlMessageHandler<PgsqlQueryRequestMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRequestHandler.class);

    public QueryRequestHandler() {
        super(PgsqlSimpleQueryMessage.class, PgsqlParseMessage.class, PgsqlBindMessage.class,
                PgsqlDescribeMessage.class, PgsqlExecuteMessage.class, PgsqlCloseMessage.class, PgsqlSyncMessage.class,
                PgsqlFlushMessage.class);
    }

    @Override
    protected boolean isStreamingSupported(byte type) {
        // don't forget to configure PgsqlRawPartAccumulator in the pipeline
        return type == PgsqlSimpleQueryMessage.TYPE;
    }

    @Override
    protected void decodeStream(ChannelHandlerContext ctx, byte type, MutableByteBufInputStream in) throws IOException {
        assert type == PgsqlSimpleQueryMessage.TYPE;
        // Skip header
        in.skip(PgsqlSimpleQueryMessage.HEADER_SIZE);
        // Decode simple query
        simpleQueryDecodeStream(ctx, in);
    }

    private void simpleQueryDecodeStream(ChannelHandlerContext ctx, MutableByteBufInputStream in) throws IOException {
        while (in.readableBytes() > 0) {
            CString newSQLCommands = null;
            // Determinate the length of the next SQL command. Wait that enough bytes are available to have a complete SQL command. However, the separator char is not required
            int len = nextSQLCommandLength(in, false, () -> in.readableBytes() > 0);
            do {
                // Read next SQL command
                ByteBuf buffer = in.readFully(len);
                boolean last = in.readableBytes() == 0;
                int strlen = last ? buffer.capacity() - 1 : buffer.capacity();
                CString sqlCommand = CString.valueOf(buffer, strlen);
                // Process SQL command
                newSQLCommands = process(ctx, newSQLCommands, sqlCommand, last);
                // Determinate the length of the next SQL command. Break the loop if there is not enough bytes for the next SQL command. The separator char is used to determinate end of the SQL command
                len = nextSQLCommandLength(in, true, () -> in.available() > 0);
                if (len == -1 && in.available() > 0 && in.available() == in.readableBytes()) {
                    len = in.available();
                }
            } while (len > 0);
            if (newSQLCommands != null && !newSQLCommands.isEmpty()) {
                if (in.readableBytes() > 0) {
                    // Ignore ready for query from backend
                    getSqlSession(ctx).addFirstQueryResponseToIgnore(QueryResponseType.READY_FOR_QUERY);
                }
                // Send query to backend
                PgsqlSimpleQueryMessage newMsg = new PgsqlSimpleQueryMessage(newSQLCommands);
                PgsqlRawContent rawMsg = new DefaultLastPgsqlRawContent(encode(ctx, newMsg, allocate(ctx, newMsg, newSQLCommands.getByteBuf())));
                ctx.fireChannelRead(rawMsg);
            }
        }
    }

    private CString process(ChannelHandlerContext ctx, CString newSQLCommands, CString sqlCommand, boolean last) throws IOException {
        CString newSQLCommand = process(ctx, sqlCommand, last);

        if (newSQLCommand != null) {
            if (newSQLCommands == null) {
                newSQLCommands = CString.valueOf(ctx.alloc().compositeBuffer(Integer.MAX_VALUE));
            }
            if (newSQLCommand == sqlCommand) {
                newSQLCommand.retain();
            }
            newSQLCommands.append(newSQLCommand);
        }
        return newSQLCommands;
    }

    @Override
    protected PgsqlQueryRequestMessage process(ChannelHandlerContext ctx, PgsqlQueryRequestMessage msg) throws IOException {
        switch (msg.getType()) {
        case PgsqlSimpleQueryMessage.TYPE: {
            return process((PgsqlSimpleQueryMessage) msg, "Simple query",
                    // Build simple SQL statement from SimpleQuery message
                    PgsqlMessageToQueryConverter::from,
                    // Process simple SQL statement
                    sqlStatement -> process(ctx, sqlStatement),
                    // Build new SimpleQuery message from simple SQL statement (only if simple SQL statement is modified)
                    PgsqlMessageToQueryConverter::to);
        }
        case PgsqlParseMessage.TYPE: {
            return this.<PgsqlParseMessage, SQLStatement, CString> process(ctx, (PgsqlParseMessage) msg, "Parse",
                    // Build SQL statement from Parse message
                    PgsqlMessageToQueryConverter::from,
                    // Process SQL statement
                    sqlStatement -> getEventProcessor(ctx).processStatement(ctx, sqlStatement),
                    // Send response to client (only if necessary)
                    nil -> sendParseCompleteResponse(ctx),
                    // Build Parse message from SQL statement (only if SQL statement is modified)
                    PgsqlMessageToQueryConverter::to);
        }
        case PgsqlBindMessage.TYPE: {
            return process(ctx, (PgsqlBindMessage) msg, "Bind",
                    // Build bind step from Bind message
                    PgsqlMessageToQueryConverter::from,
                    // Process bind step
                    bindStep -> getEventProcessor(ctx).processBindStep(ctx, bindStep),
                    // Send response to client (only if necessary)
                    nil -> sendBindCompleteResponse(ctx),
                    // Build Bind message from bind step
                    PgsqlMessageToQueryConverter::to);
        }
        case PgsqlDescribeMessage.TYPE: {
            return process(ctx, (PgsqlDescribeMessage) msg, "Describe",
                    // Build describe step from Describe message
                    PgsqlMessageToQueryConverter::from,
                    // Process describe step
                    describeStep -> getEventProcessor(ctx).processDescribeStep(ctx, describeStep),
                    // Send responses to client (only if necessary)
                    r -> sendDescribeCompleteResponses(ctx, r),
                    // Build Describe message from describe step
                    PgsqlMessageToQueryConverter::to);
        }
        case PgsqlExecuteMessage.TYPE: {
            return process(ctx, (PgsqlExecuteMessage) msg, "Execute",
                    // Build execute step from Execute message
                    PgsqlMessageToQueryConverter::from,
                    // Process execute step
                    executeStep -> getEventProcessor(ctx).processExecuteStep(ctx, executeStep),
                    // Send response to client (only if necessary)
                    r -> sendCommandCompleteResponse(ctx, r),
                    // Build Execute message from execute step
                    PgsqlMessageToQueryConverter::to);
        }
        case PgsqlCloseMessage.TYPE: {
            return process(ctx, (PgsqlCloseMessage) msg, "Close",
                    // Build close step from Close message
                    PgsqlMessageToQueryConverter::from,
                    // Process close step
                    closeStep -> getEventProcessor(ctx).processCloseStep(ctx, closeStep),
                    // Send response to client (only if necessary)
                    nil -> sendCloseCompleteResponse(ctx),
                    // Build Close message from close step
                    PgsqlMessageToQueryConverter::to);
        }
        case PgsqlSyncMessage.TYPE: {
            return process(ctx, (PgsqlSyncMessage) msg, "Sync",
                    // Build synchronize step from Sync message
                    PgsqlMessageToQueryConverter::from,
                    // Process synchronize step
                    synchronizeStep -> getEventProcessor(ctx).processSynchronizeStep(ctx, synchronizeStep),
                    // Send response to client (only if necessary)
                    trxStatus -> sendReadyForQueryResponse(ctx, trxStatus),
                    // Build Sync message from synchronize step
                    PgsqlMessageToQueryConverter::to);
        }
        case PgsqlFlushMessage.TYPE: {
            return process(ctx, (PgsqlFlushMessage) msg, "Flush",
                    // Build flush step from Flush message
                    PgsqlMessageToQueryConverter::from,
                    // Process flush step
                    flushStep -> getEventProcessor(ctx).processFlushStep(ctx, flushStep),
                    // Send response to client (only if necessary)
                    nil -> {},
                    // Build Flush message from flush step
                    PgsqlMessageToQueryConverter::to);
        }
        default:
            throw new IllegalArgumentException("msg");
        }
    }

    private SimpleSQLStatement process(ChannelHandlerContext ctx, SimpleSQLStatement sqlStatement) throws IOException {
        CString sqlCommands = sqlStatement.getSQL();
        CString newSQLCommands = sqlCommands;
        // Simple SQL statement may contain several SQL commands that must be processed one by one
        int from = 0;
        try (MutableByteBufInputStream in = new MutableByteBufInputStream(sqlCommands.getByteBuf())) {
            // For each SQL command in the simple SQL statement
            while (newSQLCommands != null && in.readableBytes() > 0) {
                // Determinate the length of the next SQL command
                // command. The separator char is not required
                int len = nextSQLCommandLength(in, false, () -> in.readableBytes() > 0);
                // Read next SQL command
                ByteBuf buffer = in.readFully(len);
                boolean last = in.readableBytes() == 0;
                int strlen = last ? buffer.capacity() - 1 : buffer.capacity();
                CString sqlCommand = CString.valueOf(buffer, strlen);
                // Process next SQL command
                newSQLCommands = process(ctx, sqlCommands, newSQLCommands, from, sqlCommand, last);
                from += len;
            }
        }
        // Return a new SQL statement only if SQL commands are modified
        return newSQLCommands == sqlCommands ? sqlStatement
             : newSQLCommands != null ? new SimpleSQLStatement(newSQLCommands)
             : null;
    }

    private interface StreamEvaluator {
        boolean available() throws IOException;
    }

    private int nextSQLCommandLength(InputStream in, boolean separatorCharRequired, StreamEvaluator stream) throws IOException {
        boolean inQuote = false;
        boolean inSingleQuote = false;
        int len = -1;
        int index = 0;
        char cp = 0;
        Character ci = null;
        in.mark(0);
        if (stream.available()) {
            ci = Character.valueOf((char) in.read());
            index ++;
        }
        while (ci != null) {
            if (ci == '"' && !inSingleQuote) {
                if (inQuote) {
                    if ((index > 0) && stream.available()) {
                        char cn = (char) in.read();
                        index ++;
                        if ((cp != '\\' && cp != '"') && cn != '"') {
                            inQuote = false;
                        }
                        cp = ci;
                        ci = Character.valueOf(cn);
                    }
                } else {
                    inQuote = true;
                    cp = ci;
                    if (stream.available()) {
                        ci = Character.valueOf((char) in.read());
                        index ++;
                    } else {
                        ci = null;
                    }
                }
            } else if (ci == '\'' && !inQuote) {
                if (inSingleQuote) {
                    if ((index > 0) && stream.available()) {
                        char cn = (char) in.read();
                        index ++;
                        if ((cp != '\\' && cp != '\'') && cn != '\'') {
                            inSingleQuote = false;
                        }
                        cp = ci;
                        ci = Character.valueOf(cn);
                    }
                } else {
                    inSingleQuote = true;
                    cp = ci;
                    if (stream.available()) {
                        ci = Character.valueOf((char) in.read());
                        index ++;
                    } else {
                        ci = null;
                    }
                }
            } else if (ci == ';' && !inQuote && !inSingleQuote) {
                int rewind = 0;
                while (stream.available()) {
                    ci = Character.valueOf((char) in.read());
                    index ++;
                    if (ci != '\r' && ci != '\n') {
                        if (ci != 0) {
                            rewind = 1;
                        }
                        break;
                    }
                }
                len = index - rewind;
                break;
            } else {
                cp = ci;
                if (stream.available()) {
                    ci = Character.valueOf((char) in.read());
                    index ++;
                } else {
                    ci = null;
                }
            }
        }
        if (len == -1 && !separatorCharRequired) {
            len = index;
        }
        in.reset();
        return len;
    }

    private CString process(ChannelHandlerContext ctx, CString sqlCommands, CString newSQLCommands, int from, CString sqlCommand, boolean last) throws IOException {
        CString newSQLCommand = process(ctx, sqlCommand, last);

        if (newSQLCommand != sqlCommand || newSQLCommands != sqlCommands) {
            if (newSQLCommands == sqlCommands) {
                if (newSQLCommand == null) {
                    newSQLCommands = null;
                } else {
                    newSQLCommands = CString.valueOf(ctx.alloc().compositeBuffer(Integer.MAX_VALUE));
                    if (from > 0) {
                        CString cs = sqlCommands.subSequence(0, from);
                        cs.retain();
                        newSQLCommands.append(cs);
                    }
                }
            }
            if (newSQLCommand != null) {
                if (newSQLCommand == sqlCommand) {
                    newSQLCommand.retain();
                }
                newSQLCommands.append(newSQLCommand);
            }
        }
        return newSQLCommands;
    }

    private CString process(ChannelHandlerContext ctx, CString sqlCommand, boolean last) throws IOException {
        SQLStatement sqlStatement = new SimpleSQLStatement(sqlCommand);
        QueriesTransferMode<SQLStatement, CString> transferMode = getEventProcessor(ctx).processStatement(ctx, sqlStatement);
        SQLStatement newSQLStatement = process(ctx, transferMode,
                response -> {
                    if (response != null) {
                        sendCommandCompleteResponse(ctx, response);
                        if (last) {
                            sendReadyForQueryResponse(ctx, (byte) 'T');
                        }
                    }
                },
                errorDetails -> {
                    sendErrorResponse(ctx, errorDetails);
                    if (last) {
                        sendReadyForQueryResponse(ctx, (byte) 'E');
                    }
                });
        return newSQLStatement != null ? newSQLStatement.getSQL() : null;
    }

    @FunctionalInterface
    private interface CheckedFunction<T, R> {
       R apply(T t) throws IOException;
    }

    private <M extends PgsqlQueryRequestMessage, Q extends Query, R> PgsqlQueryRequestMessage process(ChannelHandlerContext ctx, M msg, String prefix, Function<M, Q> queryBuilder, CheckedFunction<Q, QueriesTransferMode<Q, R>> processor, CheckedConsumer<R> responseConsumer, Function<Q, PgsqlQueryRequestMessage> msgBuilder) throws IOException {
        return process(msg, prefix, queryBuilder, query -> {
            // Process query
            QueriesTransferMode<Q, R> transferMode = processor.apply(query);
            Q newQuery = process(ctx, transferMode, responseConsumer,
                    errorDetails -> sendErrorResponse(ctx, errorDetails));
            return newQuery;
        }, msgBuilder);
    }

    private <M extends PgsqlQueryRequestMessage, Q extends Query> PgsqlQueryRequestMessage process(M msg, String prefix, Function<M, Q> queryBuilder, CheckedFunction<Q, Q> processor, Function<Q, PgsqlQueryRequestMessage> msgBuilder) throws IOException {
        PgsqlQueryRequestMessage newMsg = msg;
        Q query = queryBuilder.apply(msg);
        LOGGER.debug("{}: {}", prefix, query);
        // Process query
        Q newQuery = processor.apply(query);
        if (newQuery != query) {
            if (newQuery == null) {
                newMsg = null;
                LOGGER.trace("{} dropped", prefix);
            } else {
                newMsg = msgBuilder.apply(newQuery);
                LOGGER.trace("{} modified: original was: {}", prefix, query);
                LOGGER.trace("{} modified: new is : {}", prefix, newQuery);
            }
        }
        return newMsg;
    }

    @FunctionalInterface
    private interface CheckedConsumer<T> {
        void accept(T t) throws IOException;
    }

    private <Q extends Query, R> Q process(ChannelHandlerContext ctx, QueriesTransferMode<Q, R> transferMode, CheckedConsumer<R> responseConsumer, CheckedConsumer<Map<Byte, CString>> errorConsumer) throws IOException {
        Q newQuery;
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            for (Query query : transferMode.getNewQueries().subList(0, transferMode.getNewQueries().size() - 1)) {
                PgsqlQueryRequestMessage msg = PgsqlMessageToQueryConverter.to(query);
                sendRequest(ctx, msg);
                if (msg instanceof PgsqlSimpleQueryMessage || msg instanceof PgsqlSyncMessage) {
                    waitForResponse(ctx);
                }
            }
            newQuery = transferMode.getLastNewQuery();
            break;
        case FORGET:
            responseConsumer.accept(transferMode.getResponse());
            newQuery = null;
            break;
        case ERROR:
            errorConsumer.accept(transferMode.getErrorDetails());
            newQuery = null;
            break;
        default:
            // Should not occur
            throw new IllegalArgumentException( "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": " + transferMode.getTransferMode());
        }
        return newQuery;
    }

    private void waitForResponse(ChannelHandlerContext ctx) throws IOException {
        // Wait for response
        PgsqlSession psqlSession = getPsqlSession(ctx);
        synchronized(psqlSession) {
            try {
                psqlSession.wait();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }

    private void sendParseCompleteResponse(ChannelHandlerContext ctx) throws IOException {
        // Build parse complete message
        PgsqlParseCompleteMessage msg = new PgsqlParseCompleteMessage();
        // Send response
        sendResponse(ctx, msg);
    }

    private void sendBindCompleteResponse(ChannelHandlerContext ctx) throws IOException {
        // Build bind complete message
        PgsqlBindCompleteMessage msg = new PgsqlBindCompleteMessage();
        // Send response
        sendResponse(ctx, msg);
    }

    @SuppressWarnings("unchecked")
    private void sendDescribeCompleteResponses(ChannelHandlerContext ctx, List<?>[] responses) throws IOException {
        List<Long> parameterTypes = null;
        List<Field> rowDescription = null;
        if (responses.length == 1) {
            rowDescription = (List<Field>) responses[0];
        } else if (responses.length == 2) {
            parameterTypes = (List<Long>) responses[0];
            rowDescription = (List<Field>) responses[1];
        }
        if (parameterTypes != null) {
            sendParameterDescriptionResponse(ctx, parameterTypes);
        }
        if (rowDescription != null) {
            sendRowDescriptionResponse(ctx, rowDescription);
        } else {
            sendNoDataResponse(ctx);
        }
    }

    private void sendParameterDescriptionResponse(ChannelHandlerContext ctx, List<Long> parameterTypes) throws IOException {
        // Build parameter description message
        PgsqlParameterDescriptionMessage msg = new PgsqlParameterDescriptionMessage(parameterTypes);
        // Send response
        sendResponse(ctx, msg);
    }

    private void sendRowDescriptionResponse(ChannelHandlerContext ctx, List<Field> rowDescription) throws IOException {
        // Build row description message
        PgsqlRowDescriptionMessage msg = new PgsqlRowDescriptionMessage(rowDescription);
        // Send response
        sendResponse(ctx, msg);
    }

    private void sendNoDataResponse(ChannelHandlerContext ctx) throws IOException {
        // Build no data message
        PgsqlNoDataMessage msg = new PgsqlNoDataMessage();
        // Send response
        sendResponse(ctx, msg);
    }

    private void sendCommandCompleteResponse(ChannelHandlerContext ctx, CString response) throws IOException {
        // Build command complete message
        PgsqlCommandCompleteMessage msg = new PgsqlCommandCompleteMessage(response);
        // Send response
        sendResponse(ctx, msg);
    }

    private void sendCloseCompleteResponse(ChannelHandlerContext ctx) throws IOException {
        // Build close complete message
        PgsqlCloseCompleteMessage msg = new PgsqlCloseCompleteMessage();
        // Send response
        sendResponse(ctx, msg);
    }

    private void sendErrorResponse(ChannelHandlerContext ctx, Map<Byte, CString> errorDetails) throws IOException {
        // Build error message
        PgsqlErrorMessage msg = new PgsqlErrorMessage(errorDetails);
        // Send response
        sendResponse(ctx, msg);
    }

    private void sendReadyForQueryResponse(ChannelHandlerContext ctx, byte trxStatus) throws IOException {
        // Build ready for query message
        PgsqlReadyForQueryMessage msg = new PgsqlReadyForQueryMessage(trxStatus);
        // Send response
        sendResponse(ctx, msg);
    }

    private <M extends PgsqlQueryResponseMessage> void sendResponse(ChannelHandlerContext ctx, M msg) throws IOException {
        // Resolve writer
        PgsqlMessageWriter<M> writer = getWriter(ctx, msg.getClass());
        // Allocate buffer
        ByteBuf buffer = writer.allocate(msg);
        // Encode
        buffer = writer.write(msg, buffer);
        // Build message
        PgsqlRawContent content = new DefaultLastPgsqlRawContent(buffer);
        // Send message
        ctx.channel().writeAndFlush(content);
    }

    private <M extends PgsqlQueryRequestMessage> void sendRequest(ChannelHandlerContext ctx, M msg) throws IOException {
        // Resolve writer
        PgsqlMessageWriter<M> writer = getWriter(ctx, msg.getClass());
        // Allocate buffer
        ByteBuf buffer = writer.allocate(msg);
        // Encode
        buffer = writer.write(msg, buffer);
        // Build message
        PgsqlRawContent content = new DefaultLastPgsqlRawContent(buffer);
        // Send message
        getPsqlSession(ctx).getServerSideChannel().writeAndFlush(content);
    }

}
