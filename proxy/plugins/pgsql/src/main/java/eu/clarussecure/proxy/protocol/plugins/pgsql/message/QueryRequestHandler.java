package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage.Field;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.converter.PgsqlMessageToQueryConverter;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.CommandResults;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.QueriesTransferMode;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.Query;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession.QueryResponseType;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLStatement;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SimpleSQLStatement;
import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.DirectedMessage;
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
        Deque<List<CString>> newGroupOfDirectedSQLCommands = null;
        while (in.readableBytes() > 0
                || (newGroupOfDirectedSQLCommands != null && !newGroupOfDirectedSQLCommands.isEmpty())) {
            if (in.readableBytes() > 0) {
                // Determinate the length of the next SQL command. Wait that enough
                // bytes are available to have a complete SQL command. However, the
                // separator char is not required
                int len = nextSQLCommandLength(in, false, () -> in.readableBytes() > 0);
                do {
                    // Read next SQL command
                    ByteBuf buffer = in.readFully(len);
                    boolean last = in.readableBytes() == 0;
                    int strlen = last ? buffer.capacity() - 1 : buffer.capacity();
                    CString sqlCommand = CString.valueOf(buffer, strlen);
                    // Process SQL command
                    newGroupOfDirectedSQLCommands = process(ctx, newGroupOfDirectedSQLCommands, sqlCommand, last);
                    if (newGroupOfDirectedSQLCommands != null && newGroupOfDirectedSQLCommands.size() > 1) {
                        len = 0;
                    } else {
                        // Determinate the length of the next SQL command. Break the
                        // loop if there is not enough bytes for the next SQL command.
                        // The separator char is used to determinate end of the SQL
                        // command
                        len = nextSQLCommandLength(in, true, () -> in.available() > 0);
                        if (len == -1 && in.available() > 0 && in.available() == in.readableBytes()) {
                            len = in.available();
                        }
                    }
                } while (len > 0);
            }
            if (newGroupOfDirectedSQLCommands != null && !newGroupOfDirectedSQLCommands.isEmpty()) {
                List<CString> newDirectedSQLCommands = newGroupOfDirectedSQLCommands.poll();
                if (newDirectedSQLCommands != null && !newDirectedSQLCommands.isEmpty()) {
                    if (in.readableBytes() > 0 || !newGroupOfDirectedSQLCommands.isEmpty()) {
                        // Ignore ready for query from backends
                        getSqlSession(ctx).addFirstQueryResponseToIgnore(QueryResponseType.READY_FOR_QUERY);
                    }
                    // Send queries to backends
                    for (int i = 0; i < newDirectedSQLCommands.size(); i++) {
                        CString newSQLCommands = newDirectedSQLCommands.get(i);
                        if (newSQLCommands != null) {
                            PgsqlSimpleQueryMessage newMsg = new PgsqlSimpleQueryMessage(newSQLCommands);
                            sendRequest(ctx, newMsg, i);
                        }
                    }
                    // Wait for responses
                    waitForResponses(ctx);
                }
            }
        }
    }

    private Deque<List<CString>> process(ChannelHandlerContext ctx, Deque<List<CString>> newGroupOfSQLCommands,
            CString sqlCommand, boolean last) throws IOException {
        List<CString> newDirectedSQLCommands = process(ctx, sqlCommand, last);

        if (newDirectedSQLCommands != null) {
            if (newGroupOfSQLCommands == null) {
                newGroupOfSQLCommands = new LinkedList<>();
            }
            List<CString> newSQLCommands = newGroupOfSQLCommands.peekLast();
            if (newSQLCommands == null || newSQLCommands.size() != newDirectedSQLCommands.size()) {
                newSQLCommands = new ArrayList<>(newDirectedSQLCommands.size());
                for (int i = 0; i < newDirectedSQLCommands.size(); i++) {
                    CString newSQLCommand = null;
                    CString newDirectedSQLCommand = newDirectedSQLCommands.get(i);
                    if (newDirectedSQLCommand != null) {
                        newSQLCommand = CString.valueOf(ctx.alloc().compositeBuffer(Integer.MAX_VALUE));
                    }
                    newSQLCommands.add(newSQLCommand);
                }
                newGroupOfSQLCommands.add(newSQLCommands);
            }
            if (newDirectedSQLCommands.size() == 1 && newDirectedSQLCommands.get(0) == sqlCommand) {
                sqlCommand.retain();
            }
            for (int i = 0; i < newDirectedSQLCommands.size(); i++) {
                CString newDirectedSQLCommand = newDirectedSQLCommands.get(i);
                if (newDirectedSQLCommand != null) {
                    CString newSQLCommand = newSQLCommands.get(i);
                    newSQLCommand.append(newDirectedSQLCommand);
                }
            }
        }
        return newGroupOfSQLCommands;
    }

    @Override
    protected List<DirectedMessage<PgsqlQueryRequestMessage>> directedProcess(ChannelHandlerContext ctx,
            PgsqlQueryRequestMessage msg) throws IOException {
        switch (msg.getType()) {
        case PgsqlSimpleQueryMessage.TYPE: {
            return process((PgsqlSimpleQueryMessage) msg, "Simple query",
                    // Build simple SQL statement from SimpleQuery message
                    PgsqlMessageToQueryConverter::from,
                    // Process simple SQL statement
                    sqlStatement -> process(ctx, sqlStatement),
                    // Build new SimpleQuery message from simple SQL statement
                    // (only if simple SQL statement is modified)
                    PgsqlMessageToQueryConverter::to);
        }
        case PgsqlParseMessage.TYPE: {
            return this.<PgsqlParseMessage, SQLStatement, CommandResults>process(ctx, (PgsqlParseMessage) msg, "Parse",
                    // Build SQL statement from Parse message
                    PgsqlMessageToQueryConverter::from,
                    // Process SQL statement
                    sqlStatement -> getEventProcessor(ctx).processStatement(ctx, sqlStatement),
                    // Send responses to client (only if necessary)
                    responses -> sendCommandResults(ctx, responses),
                    // Build Parse message from SQL statement (only if SQL
                    // statement is modified)
                    PgsqlMessageToQueryConverter::to);
        }
        case PgsqlBindMessage.TYPE: {
            return process(ctx, (PgsqlBindMessage) msg, "Bind",
                    // Build bind step from Bind message
                    PgsqlMessageToQueryConverter::from,
                    // Process bind step
                    bindStep -> getEventProcessor(ctx).processBindStep(ctx, bindStep),
                    // Send responses to client (only if necessary)
                    responses -> sendCommandResults(ctx, responses),
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
                    responses -> sendCommandResults(ctx, responses),
                    // Build Describe message from describe step
                    PgsqlMessageToQueryConverter::to);
        }
        case PgsqlExecuteMessage.TYPE: {
            return process(ctx, (PgsqlExecuteMessage) msg, "Execute",
                    // Build execute step from Execute message
                    PgsqlMessageToQueryConverter::from,
                    // Process execute step
                    executeStep -> getEventProcessor(ctx).processExecuteStep(ctx, executeStep),
                    // Send responses to client (only if necessary)
                    responses -> sendCommandResults(ctx, responses),
                    // Build Execute message from execute step
                    PgsqlMessageToQueryConverter::to);
        }
        case PgsqlCloseMessage.TYPE: {
            return process(ctx, (PgsqlCloseMessage) msg, "Close",
                    // Build close step from Close message
                    PgsqlMessageToQueryConverter::from,
                    // Process close step
                    closeStep -> getEventProcessor(ctx).processCloseStep(ctx, closeStep),
                    // Send responses to client (only if necessary)
                    responses -> sendCommandResults(ctx, responses),
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
                    nil -> {
                    },
                    // Build Flush message from flush step
                    PgsqlMessageToQueryConverter::to);
        }
        default:
            throw new IllegalArgumentException("msg");
        }
    }

    private List<SimpleSQLStatement> process(ChannelHandlerContext ctx, SimpleSQLStatement sqlStatement)
            throws IOException {
        CString sqlCommands = sqlStatement.getSQL();
        Deque<List<CString>> newGroupOfDirectedSQLCommands = null;
        // Simple SQL statement may contain several SQL commands that must be
        // processed one by one
        int from = 0;
        try (MutableByteBufInputStream in = new MutableByteBufInputStream(sqlCommands.getByteBuf())) {
            // For each SQL command in the simple SQL statement
            while (in.readableBytes() > 0) {
                // Determinate the length of the next SQL command
                // command. The separator char is not required
                int len = nextSQLCommandLength(in, false, () -> in.readableBytes() > 0);
                // Read next SQL command
                ByteBuf buffer = in.readFully(len);
                boolean last = in.readableBytes() == 0;
                int strlen = last ? buffer.capacity() - 1 : buffer.capacity();
                CString sqlCommand = CString.valueOf(buffer, strlen);
                // Process next SQL command
                newGroupOfDirectedSQLCommands = process(ctx, sqlCommands, newGroupOfDirectedSQLCommands, from,
                        sqlCommand, last);
                from += len;
            }
        }
        if (newGroupOfDirectedSQLCommands != null) {
            while (newGroupOfDirectedSQLCommands.size() > 1) {
                List<CString> newDirectedSQLCommands = newGroupOfDirectedSQLCommands.poll();
                if (newDirectedSQLCommands != null && !newDirectedSQLCommands.isEmpty()) {
                    // Ignore ready for query from backends
                    getSqlSession(ctx).addFirstQueryResponseToIgnore(QueryResponseType.READY_FOR_QUERY);
                    // Send queries to backends
                    for (int i = 0; i < newDirectedSQLCommands.size(); i++) {
                        CString newSQLCommands = newDirectedSQLCommands.get(i);
                        if (newSQLCommands != null) {
                            PgsqlSimpleQueryMessage newMsg = new PgsqlSimpleQueryMessage(newSQLCommands);
                            sendRequest(ctx, newMsg, i);
                        }
                    }
                    // Wait for responses
                    waitForResponses(ctx);
                }
            }
        }
        List<CString> newDirectedSQLCommands = newGroupOfDirectedSQLCommands != null
                ? newGroupOfDirectedSQLCommands.poll() : null;
        // Return new SQL statements only if SQL commands are modified
        if (newDirectedSQLCommands != null && newDirectedSQLCommands.size() == 1
                && newDirectedSQLCommands.get(0) == sqlCommands) {
            return Collections.singletonList(sqlStatement);
        } else if (newDirectedSQLCommands != null) {
            return newDirectedSQLCommands.stream().map(nds -> nds == null ? null : new SimpleSQLStatement(nds))
                    .collect(Collectors.toList());
        }
        return null;
    }

    private interface StreamEvaluator {
        boolean available() throws IOException;
    }

    private int nextSQLCommandLength(InputStream in, boolean separatorCharRequired, StreamEvaluator stream)
            throws IOException {
        boolean inQuote = false;
        boolean inSingleQuote = false;
        int len = -1;
        int index = 0;
        char cp = 0;
        Character ci = null;
        in.mark(0);
        if (stream.available()) {
            ci = Character.valueOf((char) in.read());
            index++;
        }
        while (ci != null) {
            if (ci == '"' && !inSingleQuote) {
                if (inQuote) {
                    if ((index > 0) && stream.available()) {
                        char cn = (char) in.read();
                        index++;
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
                        index++;
                    } else {
                        ci = null;
                    }
                }
            } else if (ci == '\'' && !inQuote) {
                if (inSingleQuote) {
                    if ((index > 0) && stream.available()) {
                        char cn = (char) in.read();
                        index++;
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
                        index++;
                    } else {
                        ci = null;
                    }
                }
            } else if (ci == ';' && !inQuote && !inSingleQuote) {
                int rewind = 0;
                while (stream.available()) {
                    ci = Character.valueOf((char) in.read());
                    index++;
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
                    index++;
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

    private Deque<List<CString>> process(ChannelHandlerContext ctx, CString sqlCommands,
            Deque<List<CString>> newGroupOfSQLCommands, int from, CString sqlCommand, boolean last) throws IOException {
        List<CString> newDirectedSQLCommands = process(ctx, sqlCommand, last);

        if (newDirectedSQLCommands != null) {
            if (newGroupOfSQLCommands == null) {
                newGroupOfSQLCommands = new LinkedList<>();
            }
            List<CString> newSQLCommands = newGroupOfSQLCommands.peekLast();
            if (newDirectedSQLCommands.isEmpty() || newDirectedSQLCommands.size() > 1
                    || newDirectedSQLCommands.get(0) != sqlCommand || newSQLCommands == null
                    || newSQLCommands.get(0) != sqlCommands) {
                if (newSQLCommands == null || newSQLCommands.size() != newDirectedSQLCommands.size()) {
                    newSQLCommands = new ArrayList<>(newDirectedSQLCommands.size());
                    for (int i = 0; i < newDirectedSQLCommands.size(); i++) {
                        CString newSQLCommand = null;
                        CString newDirectedSQLCommand = newDirectedSQLCommands.get(i);
                        if (newDirectedSQLCommand != null) {
                            newSQLCommand = CString.valueOf(ctx.alloc().compositeBuffer(Integer.MAX_VALUE));
                            if (newGroupOfSQLCommands.isEmpty() && from > 0) {
                                CString cs = sqlCommands.subSequence(0, from);
                                cs.retain();
                                newSQLCommand.append(cs);
                            }
                        }
                        newSQLCommands.add(newSQLCommand);
                    }
                    newGroupOfSQLCommands.add(newSQLCommands);
                }
            }
            if (newDirectedSQLCommands.size() == 1 && newDirectedSQLCommands.get(0) == sqlCommand) {
                sqlCommand.retain();
            }
            for (int i = 0; i < newDirectedSQLCommands.size(); i++) {
                CString newDirectedSQLCommand = newDirectedSQLCommands.get(i);
                if (newDirectedSQLCommand != null) {
                    CString newSQLCommand = newSQLCommands.get(i);
                    newSQLCommand.append(newDirectedSQLCommand);
                }
            }
        }
        return newGroupOfSQLCommands;
    }

    private List<CString> process(ChannelHandlerContext ctx, CString sqlCommand, boolean last) throws IOException {
        SQLStatement sqlStatement = new SimpleSQLStatement(sqlCommand);
        QueriesTransferMode<SQLStatement, CommandResults> transferMode = getEventProcessor(ctx).processStatement(ctx,
                sqlStatement);
        List<SQLStatement> newDirectedSQLStatements = process(ctx, transferMode, responses -> {
            if (responses != null) {
                sendCommandResults(ctx, responses);
                if (last) {
                    sendReadyForQueryResponse(ctx, (byte) 'T');
                }
            }
        }, errorDetails -> {
            sendErrorResponse(ctx, errorDetails);
            if (last) {
                sendReadyForQueryResponse(ctx, (byte) 'E');
            }
        });
        return newDirectedSQLStatements != null
                ? newDirectedSQLStatements.stream().map(s -> s != null ? s.getSQL() : null).collect(Collectors.toList())
                : null;
    }

    @FunctionalInterface
    private interface CheckedFunction<T, R> {
        R apply(T t) throws IOException;
    }

    private <M extends PgsqlQueryRequestMessage, Q extends Query, R> List<DirectedMessage<PgsqlQueryRequestMessage>> process(
            ChannelHandlerContext ctx, M msg, String prefix, Function<M, Q> queryBuilder,
            CheckedFunction<Q, QueriesTransferMode<Q, R>> processor, CheckedConsumer<R> responseConsumer,
            Function<Q, PgsqlQueryRequestMessage> msgBuilder) throws IOException {
        return process(msg, prefix, queryBuilder, query -> {
            // Process query
            QueriesTransferMode<Q, R> transferMode = processor.apply(query);
            List<Q> newQueries = process(ctx, transferMode, responseConsumer,
                    errorDetails -> sendErrorResponse(ctx, errorDetails));
            return newQueries;
        }, msgBuilder);
    }

    private <M extends PgsqlQueryRequestMessage, Q extends Query> List<DirectedMessage<PgsqlQueryRequestMessage>> process(
            M msg, String prefix, Function<M, Q> queryBuilder, CheckedFunction<Q, List<Q>> processor,
            Function<Q, PgsqlQueryRequestMessage> msgBuilder) throws IOException {
        List<DirectedMessage<PgsqlQueryRequestMessage>> directedMsgs = Collections
                .singletonList(new DirectedMessage<>(0, msg));
        Q query = queryBuilder.apply(msg);
        LOGGER.debug("{}: {}", prefix, query);
        // Process queries
        List<Q> newQueries = processor.apply(query);
        if (newQueries == null || newQueries.size() != 1 || newQueries.get(0) != query) {
            if (newQueries == null || newQueries.isEmpty()) {
                directedMsgs = null;
                LOGGER.trace("{} dropped", prefix);
            } else {
                directedMsgs = new ArrayList<>(newQueries.size());
                for (int i = 0; i < newQueries.size(); i++) {
                    Q newQuery = newQueries.get(i);
                    if (newQuery != null) {
                        PgsqlQueryRequestMessage newMsg = msgBuilder.apply(newQuery);
                        LOGGER.trace("{} modified: original was: {}", prefix, query);
                        LOGGER.trace("{} modified: new is : {}", prefix, newQuery);
                        directedMsgs.add(new DirectedMessage<>(i, newMsg));
                    }
                }
            }
        }
        return directedMsgs;
    }

    @FunctionalInterface
    private interface CheckedConsumer<T> {
        void accept(T t) throws IOException;
    }

    private <Q extends Query, R> List<Q> process(ChannelHandlerContext ctx, QueriesTransferMode<Q, R> transferMode,
            CheckedConsumer<R> responseConsumer, CheckedConsumer<Map<Byte, CString>> errorConsumer) throws IOException {
        List<Q> newDirectedLastQueries;
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            int maxBackend = transferMode.getNewDirectedQueries().keySet().stream().max(Comparator.naturalOrder())
                    .get();
            List<List<Query>> newDirectedQueries = IntStream.range(0, maxBackend + 1)
                    .mapToObj(backend -> transferMode.getNewDirectedQueries().get(backend))
                    .map(l -> l == null ? Collections.<Query>emptyList() : l).collect(Collectors.toList());
            newDirectedLastQueries = new ArrayList<>(newDirectedQueries.size());
            for (int i = 0; i < newDirectedQueries.size(); i++) {
                List<Query> newQueries = newDirectedQueries.get(i);
                if (!newQueries.isEmpty()) {
                    for (Query query : newQueries.subList(0, newQueries.size() - 1)) {
                        PgsqlQueryRequestMessage msg = PgsqlMessageToQueryConverter.to(query);
                        sendRequest(ctx, msg, i);
                        if (msg instanceof PgsqlSimpleQueryMessage || msg instanceof PgsqlSyncMessage) {
                            waitForResponses(ctx);
                        }
                    }
                }
                @SuppressWarnings("unchecked")
                Q lastNewQuery = newQueries.size() > 0 ? (Q) newQueries.get(newQueries.size() - 1) : null;
                newDirectedLastQueries.add(lastNewQuery);
            }
            break;
        case FORGET:
            responseConsumer.accept(transferMode.getResponse());
            newDirectedLastQueries = null;
            break;
        case ERROR:
            errorConsumer.accept(transferMode.getErrorDetails());
            newDirectedLastQueries = null;
            break;
        default:
            // Should not occur
            throw new IllegalArgumentException(
                    "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": "
                            + transferMode.getTransferMode());
        }
        return newDirectedLastQueries;
    }

    private void waitForResponses(ChannelHandlerContext ctx) throws IOException {
        // Wait for responses
        SQLSession psqlSession = getSqlSession(ctx);
        try {
            psqlSession.waitForResponses();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private void sendCommandResults(ChannelHandlerContext ctx, CommandResults commandResults) throws IOException {
        if (commandResults.isParseCompleteRequired()) {
            sendParseCompleteResponse(ctx);
        }
        if (commandResults.isBindCompleteRequired()) {
            sendBindCompleteResponse(ctx);
        }
        if (commandResults.getParameterDescription() != null) {
            sendParameterDescriptionResponse(ctx, commandResults.getParameterDescription());
        }
        if (commandResults.getRowDescription() != null) {
            if (commandResults.getRowDescription().isEmpty()) {
                sendNoDataResponse(ctx);
            } else {
                sendRowDescriptionResponse(ctx, commandResults.getRowDescription());
            }
        }
        if (commandResults.getRows() != null) {
            for (List<ByteBuf> row : commandResults.getRows()) {
                sendDataRowResponse(ctx, row);
            }
        }
        if (commandResults.getCompleteTag() != null) {
            sendCommandCompleteResponse(ctx, commandResults.getCompleteTag());
        }
        if (commandResults.isCloseCompleteRequired()) {
            sendCloseCompleteResponse(ctx);
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

    private void sendParameterDescriptionResponse(ChannelHandlerContext ctx, List<Long> parameterTypes)
            throws IOException {
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

    private void sendDataRowResponse(ChannelHandlerContext ctx, List<ByteBuf> row) throws IOException {
        // Build data row message
        PgsqlDataRowMessage msg = new PgsqlDataRowMessage(row);
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

    private void sendReadyForQueryResponse(ChannelHandlerContext ctx, byte trxStatus) throws IOException {
        // Build ready for query message
        PgsqlReadyForQueryMessage msg = new PgsqlReadyForQueryMessage(trxStatus);
        // Send response
        sendResponse(ctx, msg);
    }

}
