package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.buffer.MutableByteBufInputStream;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.MutablePgsqlRawMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCountUtil;

public abstract class PgsqlMessageHandler <T extends PgsqlMessage> extends MessageToMessageDecoder<PgsqlRawMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgsqlMessageHandler.class);

    protected final int[] types;

    protected PgsqlMessageHandler(int... types) {
        this.types = types;
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        if (!super.acceptInboundMessage(msg)) {
            return false;
        }
        if (msg instanceof FullPgsqlRawMessage || msg instanceof MutablePgsqlRawMessage) {
            return Arrays.stream(types).anyMatch(type -> ((PgsqlRawMessage) msg).getType() == type);
        }
        return false;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, PgsqlRawMessage rawMsg, List<Object> out) throws Exception {
        if (isStreamingSupported() && rawMsg instanceof MutablePgsqlRawMessage && !((MutablePgsqlRawMessage) rawMsg).isComplete()) {
            LOGGER.trace("Decoding raw message in streaming mode: {}...", rawMsg);
            decodeStream(ctx, rawMsg);
            LOGGER.trace("Full raw message decoded: {}", rawMsg);
        } else {
            LOGGER.trace("Decoding full raw message: {}...", rawMsg);
            T pgsqlMessage = decode(ctx, rawMsg.getType(), rawMsg.getContent());
            LOGGER.trace("PGSQL message decoded: {}", pgsqlMessage);
            T newPgsqlMessage = process(ctx, pgsqlMessage);
            if (newPgsqlMessage != null) {
                if (newPgsqlMessage != pgsqlMessage) {
                    LOGGER.trace("Encoding modified PGSQL message {}...", newPgsqlMessage);
//                    ByteBufAllocator allocator = new RecyclerByteBufAllocator(rawMsg.getBytes(), ctx);
                    ByteBufAllocator allocator = ctx.alloc();
                    ByteBuf byteBuf = encode(ctx, newPgsqlMessage, allocator);
                    rawMsg = new DefaultFullPgsqlRawMessage(byteBuf, newPgsqlMessage.getType(), byteBuf.capacity());
                    LOGGER.trace("Full raw message encoded: {}", rawMsg);
                } else {
                    ReferenceCountUtil.retain(rawMsg);
                }
                out.add(rawMsg);
                LOGGER.trace("Full raw message retained in the pipeline : {}", rawMsg);
            } else {
                LOGGER.trace("Full raw message consumed {}...", rawMsg);
            }
        }
    }

    protected void decodeStream(ChannelHandlerContext ctx, PgsqlRawMessage rawMsg) throws IOException {
        LOGGER.trace("Creating input stream...");
        try (MutableByteBufInputStream in = new MutableByteBufInputStream(rawMsg.getBytes(), rawMsg.getTotalLength())) {
            LOGGER.trace("Input stream created to read from {}", rawMsg);
            // Decode and process content
            decodeStream(ctx, rawMsg.getType(), in);
        }
    }

    protected boolean isStreamingSupported() {
        // don't forget to configure PgsqlPartAccumulator in the pipeline
        return false;
    }

    protected void decodeStream(ChannelHandlerContext ctx, byte type, MutableByteBufInputStream in) throws IOException {
        throw new UnsupportedOperationException("Unsupported decoding from input stream");
    }

    protected T decode(ChannelHandlerContext ctx, byte type, ByteBuf content) throws IOException {
        throw new UnsupportedOperationException("Unsupported decoding of full raw message");
    }

    protected T process(ChannelHandlerContext ctx, T msg) throws IOException {
        throw new UnsupportedOperationException(String.format("Unsupported processing of %s message", msg.getClass().getSimpleName()));
    }

    protected ByteBuf encode(ChannelHandlerContext ctx, T msg) throws IOException {
        return encode(ctx, msg, null);
    }

    protected ByteBuf encode(ChannelHandlerContext ctx, T msg, ByteBufAllocator allocator) throws IOException {
        throw new UnsupportedOperationException(String.format("Unsupported encoding of %s message", msg.getClass().getSimpleName()));
    }

}
