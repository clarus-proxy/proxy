package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCancelRequestMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSSLRequestMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSSLResponseMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlStartupMessage;
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

    private interface MsgHeaderParser {
        byte getType(ByteBuf in);

        int getLength(ByteBuf in);

        void postProcessing(byte type);
    }

    private class FrontendFirstMsgHeaderParserImpl implements MsgHeaderParser {
        private int length;

        public FrontendFirstMsgHeaderParserImpl() {
            expectedTypeSize.set(0);
            expectedHeaderSize.set(Integer.BYTES);
        }

        @Override
        public byte getType(ByteBuf in) {
            // message type is either StartupMessage or SSLRequest
            // read message length from message header
            length = in.readInt();
            // consider that by default, message is StartupMessage
            byte type = PgsqlStartupMessage.TYPE;
            if (length == PgsqlSSLRequestMessage.LENGTH) {
                int code = in.readInt();
                if (code == PgsqlSSLRequestMessage.CODE) {
                    // message is SSLRequest
                    type = PgsqlSSLRequestMessage.TYPE;
                }
            } else if (length == PgsqlCancelRequestMessage.LENGTH) {
                int code = in.readInt();
                if (code == PgsqlCancelRequestMessage.CODE) {
                    // message is SSLRequest
                    type = PgsqlCancelRequestMessage.TYPE;
                }
            }
            return type;
        }

        @Override
        public int getLength(ByteBuf in) {
            // read message length from message header
            return length;
        }

        @Override
        public void postProcessing(byte type) {
            if (type == PgsqlStartupMessage.TYPE) {
                parser.set(new MsgHeaderParserImpl());
            }
        }
    }

    private class BackendFirstMsgHeaderParserImpl implements MsgHeaderParser {
        public BackendFirstMsgHeaderParserImpl() {
            expectedTypeSize.set(0);
            expectedHeaderSize.set(0);
        }

        @Override
        public byte getType(ByteBuf in) {
            // message is SSLResponse
            return PgsqlSSLResponseMessage.TYPE;
        }

        @Override
        public int getLength(ByteBuf in) {
            // message is SSLResponse
            return PgsqlSSLResponseMessage.LENGTH;
        }

        @Override
        public void postProcessing(byte type) {
            if (type == PgsqlSSLResponseMessage.TYPE) {
                parser.set(new MsgHeaderParserImpl());
            }
        }
    }

    private class MsgHeaderParserImpl implements MsgHeaderParser {
        public MsgHeaderParserImpl() {
            expectedTypeSize.set(Byte.BYTES);
            expectedHeaderSize.set(Byte.BYTES + Integer.BYTES);
        }

        @Override
        public byte getType(ByteBuf in) {
            // read message type from message header
            return in.readByte();
        }

        @Override
        public int getLength(ByteBuf in) {
            // read message length from message header
            return in.readInt();
        }

        @Override
        public void postProcessing(byte type) {
        }
    }

    private final String from;
    private final int maxlen;
    private AtomicInteger missing;
    private AtomicInteger expectedTypeSize;
    private AtomicInteger expectedHeaderSize;
    private AtomicReference<MsgHeaderParser> parser;

    public PgsqlRawPartDecoder(boolean frontend, int maxlen) {
        this.from = frontend ? "(F->)" : "(<-B)";
        this.maxlen = maxlen;
        this.missing = new AtomicInteger(0);
        this.expectedTypeSize = new AtomicInteger(0);
        this.expectedHeaderSize = new AtomicInteger(0);
        parser = new AtomicReference<>(
                frontend ? new FrontendFirstMsgHeaderParserImpl() : new BackendFirstMsgHeaderParserImpl());
    }

    public void skipFirstMessages() {
        if (!(parser.get() instanceof MsgHeaderParserImpl)) {
            parser.set(new MsgHeaderParserImpl());
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        LOGGER.trace("{} Decoding {} bytes...", from, in.readableBytes());
        while (in.readableBytes() > 0) {
            if (missing.get() == 0) { // New message
                if (in.readableBytes() < expectedHeaderSize.get()) {
                    return; // Message header is 4 or 5 bytes for frontend, 0 or 5 bytes for backend
                }
                in.markReaderIndex();
                // read message type
                byte type = parser.get().getType(in);
                // read message length
                int length = parser.get().getLength(in);
                in.resetReaderIndex();
                ByteBuf bytes = in.readRetainedSlice(
                        Math.min(Math.min(maxlen, in.readableBytes()), expectedTypeSize.get() + length));
                int m = expectedTypeSize.get() + length - bytes.readableBytes();
                missing.set(m);
                PgsqlRawMessage message = m == 0 ? new DefaultFullPgsqlRawMessage(bytes, type, length)
                        : new DefaultPgsqlRawMessage(bytes, type, length);
                // Redefine first, expectedTypeSize and expectedHeaderSize if necessary
                parser.get().postProcessing(type);
                out.add(message);
                LOGGER.trace("{} New {} raw message decoded: {}", from, m == 0 ? "full" : "partial", message);
            } else { // Additional content of a message
                ByteBuf content = in.readRetainedSlice(Math.min(Math.min(maxlen, in.readableBytes()), missing.get()));
                int m = missing.addAndGet(-content.readableBytes());
                DefaultPgsqlRawContent message = m == 0 ? new DefaultLastPgsqlRawContent(content)
                        : new DefaultPgsqlRawContent(content);
                out.add(message);
                LOGGER.trace("{} {} part of raw message decoded: {}", from, m == 0 ? "Last" : "New", message);
            }
        }
        LOGGER.trace("{} Remain {} bytes to decode", from, in.readableBytes());
    }

}
