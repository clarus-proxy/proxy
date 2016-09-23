package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.StatementTransferMode;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer.PgsqlMessageWriter;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.DefaultLastPgsqlRawContent;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawContent;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.buffer.MutableByteBufInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;

public class QueryHandler extends PgsqlMessageHandler<PgsqlQueryMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryHandler.class);

    public QueryHandler() {
        super(PgsqlSimpleQueryMessage.class);
    }

    @Override
    protected boolean isStreamingSupported() {
        // don't forget to configure PgsqlRawPartAccumulator in the pipeline
        return true;
    }

    @Override
    protected void decodeStream(ChannelHandlerContext ctx, byte type, MutableByteBufInputStream in) throws IOException {
        assert type == PgsqlSimpleQueryMessage.TYPE;
        // Skip header
        in.skip(PgsqlQueryMessage.HEADER_SIZE);
        switch (type) {
        case PgsqlSimpleQueryMessage.TYPE:
            // Decode simple query
            simpleQueryDecodeStream(ctx, in);
            break;
        default:
            throw new IllegalArgumentException("type");
        }
    }

    private void simpleQueryDecodeStream(ChannelHandlerContext ctx, MutableByteBufInputStream in) throws IOException {
        while (in.readableBytes() > 0) {
            CString newQuery = null;
            // Determinate the length of the next statement. Wait that enough bytes are available to have a complete statement. However, the separator char is not required
            int len = nextStatementLength(in, false, () -> in.readableBytes() > 0);
            do {
                // Read next statement
                ByteBuf buffer = in.readFully(len);
                boolean lastStatement = in.readableBytes() == 0;
                int strlen = lastStatement ? buffer.capacity() - 1 : buffer.capacity();
                CString statement = CString.valueOf(buffer, strlen);
                // Process statement
                newQuery = process(ctx, newQuery, statement, lastStatement);
                // Determinate the length of the next statement. Break the loop if there is not enough bytes for the next statement. The separator char is used to determinate end of the statement
                len = nextStatementLength(in, true, () -> in.available() > 0);
                if (len == -1 && in.available() > 0 && in.available() == in.readableBytes()) {
                    len = in.available();
                }
            } while (len > 0);
            if (newQuery != null && !newQuery.isEmpty()) {
                if (in.readableBytes() > 0) {
                    // Ignore ready for query from backend
                    getSession(ctx).incrementeReadyForQueryToIgnore();
                }
                // Send query to backend
                PgsqlSimpleQueryMessage newMsg = new PgsqlSimpleQueryMessage(newQuery);
                PgsqlRawContent rawMsg = new DefaultLastPgsqlRawContent(encode(ctx, newMsg, allocate(ctx, newMsg, newQuery.getByteBuf())));
                ctx.fireChannelRead(rawMsg);
            }
        }
    }

    private CString process(ChannelHandlerContext ctx, CString newQuery, CString statement, boolean lastStatement) throws IOException {
        CString newStatements = process(ctx, statement, lastStatement);

        if (newStatements != null && !newStatements.isEmpty()) {
            if (newQuery == null) {
                newQuery = CString.valueOf(ctx.alloc().compositeBuffer(Integer.MAX_VALUE));
            }
            if (newStatements == statement) {
                newStatements.retain();
            }
            newQuery.append(newStatements);
        }
        return newQuery;
    }

    @Override
    protected PgsqlQueryMessage process(ChannelHandlerContext ctx, PgsqlQueryMessage msg) throws IOException {
        PgsqlQueryMessage newMsg = msg;
        switch (msg.getType()) {
        case PgsqlSimpleQueryMessage.TYPE:
            CString query = msg.getQuery();
            LOGGER.debug("Simple query: {}", query);
            CString newQuery = query;
            int from = 0;
            try (MutableByteBufInputStream in = new MutableByteBufInputStream(query.getByteBuf())) {
                while (newQuery != null && in.readableBytes() > 0) {
                    // Determinate the length of the next statement. The separator char is not required
                    int len = nextStatementLength(in, false, () -> in.readableBytes() > 0);
                    // Read next statement
                    ByteBuf buffer = in.readFully(len);
                    boolean lastStatement = in.readableBytes() == 0;
                    int strlen = lastStatement ? buffer.capacity() - 1 : buffer.capacity();
                    CString statement = CString.valueOf(buffer, strlen);
                    // Process statement
                    newQuery = process(ctx, query, newQuery, from, statement, lastStatement);
                    from += len;
                }
            }
            if (newQuery != query) {
                if (newQuery == null) {
                    newMsg = null;
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Query dropped");
                    }
                } else {
                    newMsg = new PgsqlSimpleQueryMessage(newQuery);
                    if (LOGGER.isTraceEnabled()) {
                        ByteBuf bytes = query.getByteBuf();
                        LOGGER.trace("Query modified: original was memory={} content={}",
                                bytes.hasMemoryAddress() ? bytes.memoryAddress() : -1,
                                ByteBufUtil.hexDump(bytes, 0, bytes.capacity()));
                        bytes = newQuery.getByteBuf();
                        LOGGER.trace("Query modified: new is memory={} content={}",
                                bytes.hasMemoryAddress() ? bytes.memoryAddress() : -1,
                                ByteBufUtil.hexDump(bytes, 0, bytes.capacity()));
                    }
                }
            }
            break;
        default:
            throw new IllegalArgumentException("msg");
        }
        return newMsg;
    }

    private CString process(ChannelHandlerContext ctx, CString query, CString newQuery, int from, CString statement,
            boolean lastStatement) throws IOException {
        CString newStatements = process(ctx, statement, lastStatement);

        if (newStatements != statement || newQuery != query) {
            if (newQuery == query) {
                if (newStatements == null) {
                    newQuery = null;
                } else {
                    newQuery = CString.valueOf(ctx.alloc().compositeBuffer(Integer.MAX_VALUE));
                    if (from > 0) {
                        CString cs = query.subSequence(0, from);
                        cs.retain();
                        newQuery.append(cs);
                    }
                }
            }
            if (newStatements != null && !newStatements.isEmpty()) {
                if (newStatements == statement) {
                    newStatements.retain();
                }
                newQuery.append(newStatements);
            }
        }
        return newQuery;
    }

    private interface StreamEvaluator {
        boolean available() throws IOException;
    }

    private int nextStatementLength(InputStream in, boolean separatorCharRequired, StreamEvaluator stream) throws IOException {
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

    private CString process(ChannelHandlerContext ctx, CString statement, boolean lastStatement) throws IOException {
        CString newStatements;
        StatementTransferMode transferMode = getEventProcessor(ctx).processStatement(ctx, statement, lastStatement);
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            newStatements = transferMode.getNewStatements();
            break;
        case FORGET:
            if (lastStatement && transferMode.getResponse() != null) {
                sendCommandCompleteResponse(ctx, transferMode.getResponse());
                newStatements = null;
            } else {
                newStatements = CString.valueOf("");
            }
            break;
        case DENY:
            sendErrorResponse(ctx, transferMode.getResponse());
            newStatements = null;
            break;
        default:
            // Should not occur
            throw new IllegalArgumentException(
                    "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": "
                            + transferMode.getTransferMode());
        }
        if (newStatements == null) {
            sendReadyForQueryResponse(ctx);
        }
        return newStatements;
    }

    private void sendCommandCompleteResponse(ChannelHandlerContext ctx, CString response) throws IOException {
        PgsqlCommandCompleteMessage msg = new PgsqlCommandCompleteMessage(response);
        // Send response
        sendResponse(ctx, msg);
    }

    private void sendErrorResponse(ChannelHandlerContext ctx, CString errorMsg) throws IOException {
        Map<Byte, CString> fields = new LinkedHashMap<>();
        fields.put((byte) 'S', CString.valueOf("FATAL"));
        fields.put((byte) 'M', errorMsg);
        PgsqlErrorMessage msg = new PgsqlErrorMessage(fields);
        // Send response
        sendResponse(ctx, msg);
    }

    private void sendReadyForQueryResponse(ChannelHandlerContext ctx) throws IOException {
        PgsqlReadyForQueryMessage msg = new PgsqlReadyForQueryMessage((byte) 'T');
        // Send response
        sendResponse(ctx, msg);
    }

    private <M extends PgsqlQueryResponseMessage> void sendResponse(ChannelHandlerContext ctx, M msg) throws IOException {
        // Resolve writer
        PgsqlMessageWriter<M> writer = getWriter(ctx, msg.getClass());
        // Compute total length
        int total = writer.length(msg);
        // Allocate buffer
        ByteBuf buffer = ctx.alloc().buffer(total);
        // Encode
        buffer = writer.write(msg, buffer);
        // Build message
        PgsqlRawContent content = new DefaultLastPgsqlRawContent(buffer);
        // Send message
        ctx.channel().writeAndFlush(content);
    }

}
