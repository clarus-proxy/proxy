package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConstants;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.CommandProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.CommandResultTransferMode;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.ReadyForQueryResponseTransferMode;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlMessageHandler;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;

public class QueryResponseHandler extends PgsqlMessageHandler<PgsqlQueryResponseMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResponseHandler.class);

    public QueryResponseHandler() {
        super(PgsqlCommandCompleteMessage.TYPE, PgsqlErrorMessage.TYPE, PgsqlReadyForQueryMessage.TYPE);
    }

    @Override
    protected PgsqlQueryResponseMessage decode(ChannelHandlerContext ctx, byte type, ByteBuf content) throws IOException {
        switch (type) {
        case PgsqlCommandCompleteMessage.TYPE:
            return decodeCommandCompleteMessage(ctx, content);
        case PgsqlErrorMessage.TYPE:
            return decodeErrorMessage(ctx, content);
        case PgsqlReadyForQueryMessage.TYPE:
            return decodeReadyForQueryMessage(ctx, content);
        default:
            throw new IllegalArgumentException("type");
        }
    }

    private PgsqlCommandCompleteMessage decodeCommandCompleteMessage(ChannelHandlerContext ctx, ByteBuf content) {
        // Read tag
        CString tag = PgsqlUtilities.getCString(content);
        return new PgsqlCommandCompleteMessage(tag);
    }

    private PgsqlErrorMessage decodeErrorMessage(ChannelHandlerContext ctx, ByteBuf content) {
        // Read fields
        Map<Byte, CString> fields = new LinkedHashMap<>();
        byte code = content.readByte();
        while (code != 0) {
            CString value = PgsqlUtilities.getCString(content);
            fields.put(code, value);
            code = content.readByte();
        }
        return new PgsqlErrorMessage(fields);
    }

    private PgsqlReadyForQueryMessage decodeReadyForQueryMessage(ChannelHandlerContext ctx, ByteBuf content) {
        // Read transaction status
        byte transactionStatus = content.readByte();
        return new PgsqlReadyForQueryMessage(transactionStatus);
    }

    @Override
    protected PgsqlQueryResponseMessage process(ChannelHandlerContext ctx, PgsqlQueryResponseMessage msg) throws IOException {
        switch (msg.getType()) {
        case PgsqlCommandCompleteMessage.TYPE:
        case PgsqlErrorMessage.TYPE:
        {
            PgsqlCommandResultMessage resultMsg = (PgsqlCommandResultMessage) msg;
            PgsqlCommandResultMessage newMsg = resultMsg;
            CString details = resultMsg.getDetails();
            String prefix = resultMsg.getType() == PgsqlCommandCompleteMessage.TYPE ? "Command complete" : "Error";
            LOGGER.debug("{}: {}", prefix, details);
            CString newDetails = processCommandResult(ctx, details);
            if (newDetails != details) {
                if (newDetails == null) {
                    newMsg = null;
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("{} dropped", prefix);
                    }
                } else {
                    if (PgsqlErrorMessage.isErrorFields(newDetails)) {
                        newMsg = new PgsqlErrorMessage(newDetails);
                    } else {
                        newMsg = new PgsqlCommandCompleteMessage(newDetails);
                    }
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Command result modified: original was {}: {}", prefix, details);
                        prefix = newMsg.getType() == PgsqlCommandCompleteMessage.TYPE ? "Command complete" : "Error";
                        LOGGER.trace("Command result modified: new is {}: {}", prefix, newDetails);
                    }
                }
            }
            return newMsg;
        }
        case PgsqlReadyForQueryMessage.TYPE:
        {
            PgsqlReadyForQueryMessage readyMsg = (PgsqlReadyForQueryMessage) msg;
            PgsqlReadyForQueryMessage newMsg = readyMsg;
            Byte transactionStatus = readyMsg.getTransactionStatus();
            LOGGER.debug("Ready for query: {}", (char) transactionStatus.byteValue());
            Byte newTransactionStatus = processReadyForQueryResponse(ctx, transactionStatus);
            if (newTransactionStatus != transactionStatus) {
                if (newTransactionStatus == null) {
                    newMsg = null;
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Ready for query dropped");
                    }
                } else {
                    newMsg = new PgsqlReadyForQueryMessage(newTransactionStatus);
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Ready for query modified: original transaction status was {}", (char) transactionStatus.byteValue());
                        LOGGER.trace("Ready for query modified: new transaction status is {}", (char) newTransactionStatus.byteValue());
                    }
                }
            }
            return newMsg;
        }
        default:
            throw new IllegalArgumentException("msg");
        }
    }

    private CString processCommandResult(ChannelHandlerContext ctx, CString details) throws IOException {
        CString newDetails;
        CommandResultTransferMode transferMode = getCommandProcessor(ctx).processCommandResult(ctx, details);
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            newDetails = transferMode.getNewDetails();
            break;
        case FORGET:
            newDetails = null;
            break;
        case DENY:
        default:
            // Should not occur
            throw new IllegalArgumentException("Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": " + transferMode.getTransferMode());
        }

        return newDetails;
    }

    private Byte processReadyForQueryResponse(ChannelHandlerContext ctx, Byte transactionStatus) throws IOException {
        Byte newTransactionStatus;
        ReadyForQueryResponseTransferMode transferMode = getCommandProcessor(ctx).processReadyForQueryResponse(ctx, transactionStatus);
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            newTransactionStatus = transferMode.getNewTransactionStatus();
            break;
        case FORGET:
            newTransactionStatus = null;
            break;
        case DENY:
        default:
            // Should not occur
            throw new IllegalArgumentException("Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": " + transferMode.getTransferMode());
        }

        return newTransactionStatus;
    }

    @Override
    protected ByteBuf encode(ChannelHandlerContext ctx, PgsqlQueryResponseMessage msg, ByteBufAllocator allocator) throws IOException {
        switch (msg.getType()) {
        case PgsqlCommandCompleteMessage.TYPE:
            return encodeCommandCompleteMessage(ctx, (PgsqlCommandCompleteMessage)msg, allocator);
        case PgsqlErrorMessage.TYPE:
            return encodeErrorMessage(ctx, (PgsqlErrorMessage)msg, allocator);
        case PgsqlReadyForQueryMessage.TYPE:
            return encodeReadyForQueryMessage(ctx, (PgsqlReadyForQueryMessage)msg, allocator);
        default:
            throw new IllegalArgumentException("msg");
        }
    }

    private ByteBuf encodeCommandCompleteMessage(ChannelHandlerContext ctx, PgsqlCommandCompleteMessage msg, ByteBufAllocator allocator) throws IOException {
        // Compute total length
        int total = msg.getHeaderSize() + msg.getTag().clen();
        // Allocate buffer
        ByteBuf buffer = allocator.buffer(total);
        // Write header (type + length)
        buffer.writeByte(msg.getType());
        // Compute length
        int len = total - Byte.BYTES;
        buffer.writeInt(len);
        // Write tag
        ByteBuf value = msg.getTag().getByteBuf(ctx.alloc());
        buffer.writeBytes(value);
        value.release();
        return buffer;
    }

    private ByteBuf encodeErrorMessage(ChannelHandlerContext ctx, PgsqlErrorMessage msg, ByteBufAllocator allocator) throws IOException {
        // Compute total length
        int total = msg.getHeaderSize();
        for (Map.Entry<Byte, CString> field : msg.getFields().entrySet()) {
            total += Byte.BYTES;
            total += field.getValue().clen();
        }
        total += Byte.BYTES;
        // Allocate buffer
        ByteBuf buffer = allocator.buffer(total);
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
        return buffer;
    }

    private ByteBuf encodeReadyForQueryMessage(ChannelHandlerContext ctx, PgsqlReadyForQueryMessage msg, ByteBufAllocator allocator) throws IOException {
        // Compute total length
        int total = msg.getHeaderSize() + Byte.BYTES;
        // Allocate buffer
        ByteBuf buffer = ctx.alloc().buffer(total);
        // Write header (type + length)
        buffer.writeByte(msg.getType());
        // Compute length
        int len = total - Byte.BYTES;
        buffer.writeInt(len);
        // Write transaction status
        buffer.writeByte(msg.getTransactionStatus());
        return buffer;
    }

    private CommandProcessor getCommandProcessor(ChannelHandlerContext ctx) {
        PgsqlSession session = ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        return session.getCommandProcessor();
    }

}
