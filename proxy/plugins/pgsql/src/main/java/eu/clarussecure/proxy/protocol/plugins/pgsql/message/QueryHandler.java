package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConstants;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.buffer.MutableByteBufInputStream;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.CommandProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.StatementTransferMode;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.TransferMode;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.DefaultLastPgsqlRawContent;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlMessageHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawContent;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;

public class QueryHandler extends PgsqlMessageHandler<PgsqlQueryMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryHandler.class);

    public QueryHandler() {
        super(PgsqlSimpleQueryMessage.TYPE);
    }

//    @Override
//    protected boolean isStreamingSupported() {
//        // don't forget to configure PgsqlPartAccumulator in the pipeline
//        return true;
//    }

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
//        ByteBufAllocator allocator = new RecyclerByteBufAllocator(in.buffer(), ctx);
        ByteBufAllocator allocator = ctx.alloc();
        while (in.readableBytes() > 0) {
            // Read statement
            ByteBuf buffer = in.readUntil((byte) ';', (byte) 0);
            int extra = 0;
            in.mark(0);
            while (in.readableBytes() > 0) {
                byte b = in.readByte();
                if (b != '\r' && b != '\n' && b != 0) {
                    break;
                } else {
                    extra ++;
                }
            }
            in.reset();
            if (extra > 0) {
                buffer = allocator.compositeBuffer(2).addComponent(true, buffer).addComponent(true, in.readFully(extra));
            }
            CString statement = CString.valueOf(buffer);
            boolean lastStatement = in.readableBytes() == 0;
            // Process query
            StatementTransferMode transferMode = process(ctx, statement, lastStatement, true);
            switch (transferMode.getTransferMode()) {
            case FORWARD:
                PgsqlSimpleQueryMessage newMsg = new PgsqlSimpleQueryMessage(transferMode.getNewStatements());
                PgsqlRawContent rawMsg = new DefaultLastPgsqlRawContent(encode(ctx, newMsg, allocator));
                ctx.fireChannelRead(rawMsg);
                break;
            case FORGET:
                if (lastStatement && transferMode.getResponse() != null) {
                    sendCommandCompleteResponse(ctx, transferMode.getResponse());
                }
                break;
            case DENY:
                sendErrorResponse(ctx, transferMode.getResponse());
                sendReadyForQueryResponse(ctx);
                return;
            default:
                // Should not occur
                throw new IllegalArgumentException("Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": " + transferMode.getTransferMode());
            }
        }
    }

    private StatementTransferMode process(ChannelHandlerContext ctx, CString query, boolean lastStatement, boolean streaming) throws IOException {
        return getCommandProcessor(ctx).processStatement(ctx, query, lastStatement, streaming);
    }

    @Override
    protected PgsqlQueryMessage decode(ChannelHandlerContext ctx, byte type, ByteBuf content) throws IOException {
        switch (type) {
        case PgsqlSimpleQueryMessage.TYPE:
            // Read query
            CString query = PgsqlUtilities.getCString(content);
            return new PgsqlSimpleQueryMessage(query);
        default:
            throw new IllegalArgumentException("type");
        }
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
            boolean inQuote = false;
            boolean inSingleQuote = false;
            for (int i = 0; newQuery != null && i < query.length(); i ++) {
                if (query.charAt(i) == '"' && !inSingleQuote) {
                    if (inQuote) {
                        if ((i > 0 && query.charAt(i - 1) != '\\' && query.charAt(i - 1) != '"') && (i + 1 < query.length() && query.charAt(i + 1) != '"')) {
                            inQuote = false;
                        }
                    } else {
                        inQuote = true;
                    }
                } else if (query.charAt(i) == '\'' && !inQuote) {
                    if (inSingleQuote) {
                        if ((i > 0 && query.charAt(i - 1) != '\\' && query.charAt(i - 1) != '\'') && (i + 1 < query.length() && query.charAt(i + 1) != '\'')) {
                            inSingleQuote = false;
                        }
                    } else {
                        inSingleQuote = true;
                    }
                } else if (query.charAt(i) == ';' && !inQuote && !inSingleQuote) {
                    while (i + 1 < query.length() && (query.charAt(i + 1) == '\r' || query.charAt(i + 1) == '\n')) {
                        i ++;
                    }
                    if (i + 1 == query.length()) {
                        i ++;
                    }
                    CString statement = query.subSequence(from, i + 1);
                    // Process statement
                    newQuery = process(ctx, query, newQuery, from, statement, i == query.length());
                    from = i + 1;
                }
            }
            if (newQuery != null && from < query.length()) {
                CString statement = query.subSequence(from, query.clen());
                newQuery = process(ctx, query, newQuery, from, statement, true);
            }
            if (newQuery != query) {
                if (newQuery == null || newQuery.isEmpty()) {
                    sendReadyForQueryResponse(ctx);
                    newMsg = null;
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Query dropped");
                    }
                } else {
                    newMsg = new PgsqlSimpleQueryMessage(newQuery);
                    if (LOGGER.isTraceEnabled()) {
                        ByteBuf bytes = query.getByteBuf(ctx.alloc());
                        LOGGER.trace("Query modified: original was memory={} content={}", bytes.hasMemoryAddress() ? bytes.memoryAddress() : -1, ByteBufUtil.hexDump(bytes, 0, bytes.capacity()));
                        bytes = newQuery.getByteBuf(ctx.alloc());
                        LOGGER.trace("Query modified: new is memory={} content={}", bytes.hasMemoryAddress() ? bytes.memoryAddress() : -1, ByteBufUtil.hexDump(bytes, 0, bytes.capacity()));
                    }
                }
            }
            break;
        default:
            throw new IllegalArgumentException("msg");
        }
        return newMsg;
    }

    private CString process(ChannelHandlerContext ctx, CString query, CString newQuery, int from, CString statement, boolean lastStatement)
            throws IOException {
        CString newStatements;
        StatementTransferMode transferMode = process(ctx, statement, lastStatement, false);
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            newStatements = transferMode.getNewStatements();
            break;
        case FORGET:
            if (lastStatement && transferMode.getResponse() != null) {
                sendCommandCompleteResponse(ctx, transferMode.getResponse());
            }
            newStatements = null;
            break;
        case DENY:
            sendErrorResponse(ctx, transferMode.getResponse());
            newStatements = null;
            break;
        default:
            // Should not occur
            throw new IllegalArgumentException("Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": " + transferMode.getTransferMode());
        }

        if (newStatements != statement || newQuery != query) {
            if (newQuery == query) {
                if (transferMode.getTransferMode() == TransferMode.DENY) {
                    newQuery = null;
                } else {
                    if (from > 0) {
                        newQuery = CString.valueOf(ctx.alloc().buffer(query.clen()));
                        newQuery.append(query, from, ctx.alloc());
                    } else {
                        newQuery = CString.valueOf("");
                    }
                }
            }
            if (newStatements != null) {
                if (newQuery.isEmpty()) {
                    newQuery = CString.valueOf(ctx.alloc().buffer(query.clen() - from));
                }
                newQuery.append(newStatements, ctx.alloc());
            }
        }
        return newQuery;
    }

    private void sendCommandCompleteResponse(ChannelHandlerContext ctx, CString response) throws IOException {
        PgsqlCommandCompleteMessage msg = new PgsqlCommandCompleteMessage(response);
        // Compute total length
        int total = msg.getHeaderSize() + msg.getTag().clen();
        // Allocate buffer
        ByteBuf buffer = ctx.alloc().buffer(total);
        // Write header (type + length)
        buffer.writeByte(msg.getType());
        // Compute length
        int len = total - Byte.BYTES;
        buffer.writeInt(len);
        // Write tag
        ByteBuf value = msg.getTag().getByteBuf(ctx.alloc());
        buffer.writeBytes(value);
        value.release();
        PgsqlRawContent content = new DefaultLastPgsqlRawContent(buffer);
        ctx.channel().writeAndFlush(content);
    }

    private void sendErrorResponse(ChannelHandlerContext ctx, CString errorMsg) throws IOException {
        Map<Byte, CString> fields = new LinkedHashMap<>();
        fields.put((byte) 'S', CString.valueOf("FATAL"));
        fields.put((byte) 'M', errorMsg);
        PgsqlErrorMessage msg = new PgsqlErrorMessage(fields);
        // Compute total length
        int total = msg.getHeaderSize();
        for (Map.Entry<Byte, CString> field : msg.getFields().entrySet()) {
            total += Byte.BYTES;
            total += field.getValue().clen();
        }
        total += Byte.BYTES;
        // Allocate buffer
        ByteBuf buffer = ctx.alloc().buffer(total);
        // Write header (type + length)
        buffer.writeByte(msg.getType());
        // Compute length
        int len = total - Byte.BYTES;
        buffer.writeInt(len);
        // Write fields
        for (Map.Entry<Byte, CString> field : msg.getFields().entrySet()) {
            byte key = field.getKey().byteValue();
            ByteBuf value = field.getValue().getByteBuf(ctx.alloc());
            buffer.writeByte(key);
            buffer.writeBytes(value);
            value.release();
        }
        buffer.writeByte(0);
        PgsqlRawContent content = new DefaultLastPgsqlRawContent(buffer);
        ctx.channel().writeAndFlush(content);
    }

    private void sendReadyForQueryResponse(ChannelHandlerContext ctx) throws IOException {
        PgsqlReadyForQueryMessage msg = new PgsqlReadyForQueryMessage((byte) 'T');
        // Compute total length
        int total = msg.getHeaderSize() + Byte.BYTES;
        // Allocate buffer
        ByteBuf buffer = ctx.alloc().buffer(total);
        // Write header (type + length)
        buffer.writeByte(msg.getType());
        // Compute length
        int len = total - Byte.BYTES;
        buffer.writeInt(len);
        // Write tag
        buffer.writeByte(msg.getTransactionStatus());
        PgsqlRawContent content = new DefaultLastPgsqlRawContent(buffer);
        ctx.channel().writeAndFlush(content);
    }

    @Override
    protected ByteBuf encode(ChannelHandlerContext ctx, PgsqlQueryMessage msg, ByteBufAllocator allocator) throws IOException {
        if (msg instanceof PgsqlSimpleQueryMessage) {
            // Compute total length
            int total = msg.getHeaderSize() + msg.getQuery().clen();
            // Allocate buffer
            ByteBuf buffer = allocator.buffer(total);
            // Write header (type + length)
            buffer.writeByte(msg.getType());
            // Compute length
            int len = total - Byte.BYTES;
            buffer.writeInt(len);
            // Write query
            ByteBuf value = msg.getQuery().getByteBuf(ctx.alloc());
            buffer.writeBytes(value);
            value.release();
            // Fix zero byte if necessary
            if (buffer.writableBytes() == 1) {
                buffer.writeByte(0);
            }
            return buffer;
        } else {
            throw new IllegalArgumentException("msg");
        }
    }

    private CommandProcessor getCommandProcessor(ChannelHandlerContext ctx) {
        PgsqlSession session = ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        return session.getCommandProcessor();
    }

}
