package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.DefaultFullPgsqlRawMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.DefaultLastPgsqlRawContent;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.DefaultPgsqlRawContent;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.DefaultPgsqlRawMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class PgsqlRawPartDecoder extends ByteToMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgsqlRawPartDecoder.class);

    private final String from;
    private boolean first;
    private final int maxlen;
    private int missing;

    public PgsqlRawPartDecoder(boolean frontend, int maxlen) {
        this.from = frontend ? "(F->)" : "(<-B)";
        this.first = frontend;
        this.maxlen = maxlen;
        this.missing = 0;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        LOGGER.trace("{} Decoding {} bytes...", from, in.readableBytes());
        while (in.readableBytes() > 0) {
            if (missing == 0) { // New message
                if (in.readableBytes() < (first ? 0 : Byte.BYTES) + Integer.BYTES) {
                    return; // Message header is 4 or 5 bytes
                }
                in.markReaderIndex();
                byte type = first ? 0 : in.readByte();
                int length = in.readInt();
                in.resetReaderIndex();
                ByteBuf bytes = in.readRetainedSlice(Math.min(Math.min(maxlen, in.readableBytes()), (first ? 0 : Byte.BYTES) + length));
                missing = (first ? 0 : Byte.BYTES) + length - bytes.readableBytes();
                PgsqlRawMessage message = missing == 0 ? new DefaultFullPgsqlRawMessage(bytes, type, length) : new DefaultPgsqlRawMessage(bytes, type, length);
                out.add(message);
                first = false;
                LOGGER.trace("{} New {} raw message decoded: {}", from, missing == 0 ? "full" : "partial", message);
            } else { // Additional content of a message
                ByteBuf content = in.readRetainedSlice(Math.min(Math.min(maxlen, in.readableBytes()), missing));
                missing -= content.readableBytes();
                DefaultPgsqlRawContent message = missing == 0 ? new DefaultLastPgsqlRawContent(content) : new DefaultPgsqlRawContent(content);
                out.add(message);
                LOGGER.trace("{} {} part of raw message decoded: {}", from, missing == 0 ? "Last" : "New", message);
            }
        }
        LOGGER.trace("{} Remain {} bytes to decode", from, in.readableBytes());
    }

}
