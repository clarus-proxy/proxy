<<<<<<< HEAD
package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            expectedTypeSize = 0;
            expectedHeaderSize = Integer.BYTES;
        }

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
            }
            return type;
        }

        public int getLength(ByteBuf in) {
            // read message length from message header
            return length;
        }

        public void postProcessing(byte type) {
            if (type == PgsqlStartupMessage.TYPE) {
                parser = new MsgHeaderParserImpl();
//                if (otherSideDecoder != null) {
//                    otherSideDecoder.skipFirstMessages();
//                }
            }
        }
    }

    private class BackendFirstMsgHeaderParserImpl implements MsgHeaderParser {
        public BackendFirstMsgHeaderParserImpl() {
            expectedTypeSize = 0;
            expectedHeaderSize = 0;
        }

        public byte getType(ByteBuf in) {
            // message is SSLResponse
            return PgsqlSSLResponseMessage.TYPE; 
        }        

        public int getLength(ByteBuf in) {
            // message is SSLResponse
            return PgsqlSSLResponseMessage.LENGTH;
        }

        public void postProcessing(byte type) {
            if (type == PgsqlSSLResponseMessage.TYPE) {
                parser = new MsgHeaderParserImpl();
            }
        }
    }

    private class MsgHeaderParserImpl  implements MsgHeaderParser {
        public MsgHeaderParserImpl() {
            expectedTypeSize = Byte.BYTES;
            expectedHeaderSize = expectedTypeSize + Integer.BYTES;
        }

        public byte getType(ByteBuf in) {
            // read message type from message header
            return in.readByte();
        }

        public int getLength(ByteBuf in) {
            // read message length from message header
            return in.readInt();
        }

        public void postProcessing(byte type) {
        }
    }

    private final String from;
    private final int maxlen;
    private volatile int missing;
    private volatile int expectedTypeSize;
    private volatile int expectedHeaderSize;
    private volatile MsgHeaderParser parser;
//    private volatile PgsqlRawPartDecoder otherSideDecoder;

    public PgsqlRawPartDecoder(boolean frontend, int maxlen) {
        this.from = frontend ? "(F->)" : "(<-B)";
        this.maxlen = maxlen;
        this.missing = 0;
        parser = frontend ? new FrontendFirstMsgHeaderParserImpl() : new BackendFirstMsgHeaderParserImpl();
    }

//    public void setOtherSideDecoder(PgsqlRawPartDecoder decoder) {
//        this.otherSideDecoder = decoder;
//    }

    public void skipFirstMessages() {
        if (!(parser instanceof MsgHeaderParserImpl)) {
            parser = new MsgHeaderParserImpl();
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        LOGGER.trace("{} Decoding {} bytes...", from, in.readableBytes());
        while (in.readableBytes() > 0) {
            if (missing == 0) { // New message
                if (in.readableBytes() < expectedHeaderSize) {
                    return; // Message header is 4 or 5 bytes for frontend, 0 or 5 bytes for backend
                }
                in.markReaderIndex();
                // read message type
                byte type = parser.getType(in);
                // read message length
                int length = parser.getLength(in);
                in.resetReaderIndex();
                ByteBuf bytes = in.readRetainedSlice(Math.min(Math.min(maxlen, in.readableBytes()), expectedTypeSize + length));
                missing = expectedTypeSize + length - bytes.readableBytes();
                PgsqlRawMessage message = missing == 0 ? new DefaultFullPgsqlRawMessage(bytes, type, length) : new DefaultPgsqlRawMessage(bytes, type, length);
                // Redefine first, expectedTypeSize and expectedHeaderSize if necessary
                parser.postProcessing(type);
                out.add(message);
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
=======
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
>>>>>>> refs/heads/feature/pgsql_ssl
