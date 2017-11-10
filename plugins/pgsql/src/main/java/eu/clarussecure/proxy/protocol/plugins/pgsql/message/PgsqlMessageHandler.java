package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConfiguration;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConstants;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser.PgsqlMessageParser;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.EventProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer.PgsqlMessageWriter;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.DefaultFullPgsqlRawMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.DefaultLastPgsqlRawContent;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.FullPgsqlRawMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawContent;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.MutablePgsqlRawMessage;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.DirectedMessage;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.buffer.MutableByteBufInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCountUtil;

public abstract class PgsqlMessageHandler<T extends PgsqlMessage> extends MessageToMessageDecoder<PgsqlRawMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgsqlMessageHandler.class);

    protected final Map<Byte, Class<? extends T>> msgTypes;

    protected int numberOfPeerChannels = 0;
    protected int preferredPeerChannel = Integer.MIN_VALUE;

    @SafeVarargs
    protected PgsqlMessageHandler(Class<? extends T>... msgTypes) {
        this.msgTypes = Arrays.stream(msgTypes).collect(Collectors.toMap(msgType -> {
            try {
                return msgType.getField("TYPE").getByte(null);
            } catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
                // Should not occur
                LOGGER.error("Cannot read TYPE field of message class {}: ", msgType.getSimpleName(), e);
                throw new IllegalArgumentException(
                        String.format("Cannot read TYPE field of message class %s: ", msgType.getSimpleName(), e));
            }
        }, msgType -> msgType));
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        if (!super.acceptInboundMessage(msg)) {
            return false;
        }
        if (msg instanceof FullPgsqlRawMessage || msg instanceof MutablePgsqlRawMessage) {
            return msgTypes.keySet().stream().anyMatch(type -> ((PgsqlRawMessage) msg).getType() == type);
        }
        return false;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof DirectedMessage) {
            Object msg2 = ((DirectedMessage<?>) msg).getMsg();
            if (acceptInboundMessage(msg2)) {
                msg = msg2;
            }
        }
        super.channelRead(ctx, msg);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, PgsqlRawMessage rawMsg, List<Object> out) throws Exception {
        // decode message
        if (isStreamingSupported(rawMsg.getType()) && rawMsg instanceof MutablePgsqlRawMessage
                && !((MutablePgsqlRawMessage) rawMsg).isComplete()) {
            LOGGER.trace("Decoding raw message in streaming mode: {}...", rawMsg);
            decodeStream(ctx, rawMsg);
            LOGGER.trace("Full raw message decoded: {}", rawMsg);
        } else {
            LOGGER.trace("Decoding full raw message: {}...", rawMsg);
            T pgsqlMessage = decode(ctx, rawMsg.getType(), rawMsg.getContent());
            LOGGER.trace("PGSQL message decoded: {}", pgsqlMessage);
            if (getNumberOfPeerChannels(ctx) > 1) {
                List<DirectedMessage<T>> directedMsgs = directedProcess(ctx, pgsqlMessage);
                if (directedMsgs != null) {
                    ByteBuf bufferToRecycle = rawMsg.getBytes();
                    for (DirectedMessage<T> directedMsg : directedMsgs) {
                        int to = directedMsg.getTo();
                        T newPgsqlMessage = directedMsg.getMsg();
                        if (newPgsqlMessage != pgsqlMessage) {
                            LOGGER.trace("Encoding modified PGSQL message {}...", newPgsqlMessage);
                            ByteBuf buffer = allocate(ctx, newPgsqlMessage, bufferToRecycle);
                            buffer = encode(ctx, newPgsqlMessage, buffer);
                            rawMsg = new DefaultFullPgsqlRawMessage(buffer, newPgsqlMessage.getType(),
                                    buffer.capacity());
                            LOGGER.trace("Full raw message encoded: {}", rawMsg);
                            bufferToRecycle = null;
                        } else {
                            ReferenceCountUtil.retain(rawMsg);
                        }
                        out.add(new DirectedMessage<PgsqlRawMessage>(to, rawMsg));
                        LOGGER.trace("Full raw message retained in the pipeline : {}", rawMsg);
                    }
                } else {
                    LOGGER.trace("Full raw message consumed {}...", rawMsg);
                }
            } else {
                T newPgsqlMessage = process(ctx, pgsqlMessage);
                if (newPgsqlMessage != null) {
                    if (newPgsqlMessage != pgsqlMessage) {
                        LOGGER.trace("Encoding modified PGSQL message {}...", newPgsqlMessage);
                        ByteBuf buffer = allocate(ctx, newPgsqlMessage, rawMsg.getBytes());
                        buffer = encode(ctx, newPgsqlMessage, buffer);
                        rawMsg = new DefaultFullPgsqlRawMessage(buffer, newPgsqlMessage.getType(), buffer.capacity());
                        LOGGER.trace("Full raw message encoded: {}", rawMsg);
                    } else {
                        ReferenceCountUtil.retain(rawMsg);
                    }
                    rawMsg.filter(false);
                    out.add(rawMsg);
                    LOGGER.trace("Full raw message retained in the pipeline : {}", rawMsg);
                } else {
                    LOGGER.trace("Full raw message consumed {}...", rawMsg);
                }
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

    protected boolean isStreamingSupported(byte type) {
        // don't forget to configure PgsqlRawPartAccumulator in the pipeline
        return false;
    }

    protected void decodeStream(ChannelHandlerContext ctx, byte type, MutableByteBufInputStream in) throws IOException {
        throw new UnsupportedOperationException("Unsupported decoding from input stream");
    }

    protected T decode(ChannelHandlerContext ctx, byte type, ByteBuf content) throws IOException {
        Class<? extends T> msgType = msgTypes.get(type);
        if (msgType == null) {
            // Should not occur
            LOGGER.error("Unsupported decoding of full raw message for type {}", type);
            throw new UnsupportedOperationException(
                    String.format("Unsupported decoding of full raw message for type %d", type));
        }
        // Resolve parser
        PgsqlMessageParser<T> parser = getParser(ctx, msgType);
        // Parse content
        content.markReaderIndex();
        T msg = parser.parse(content);
        content.resetReaderIndex();
        return msg;
    }

    protected List<DirectedMessage<T>> directedProcess(ChannelHandlerContext ctx, T msg) throws IOException {
        T newMsg = process(ctx, msg);
        if (newMsg == null) {
            return null;
        }
        // forward message to the preferred peer channel
        int preferredPeerChannel = getPreferredPeerChannel(ctx);
        DirectedMessage<T> directedMsg = new DirectedMessage<>(preferredPeerChannel, newMsg);
        List<DirectedMessage<T>> directedMsgs = Collections.singletonList(directedMsg);
        return directedMsgs;
    }

    protected T process(ChannelHandlerContext ctx, T msg) throws IOException {
        List<DirectedMessage<T>> directedMsgs = directedProcess(ctx, msg);
        if (directedMsgs != null && directedMsgs.size() > 1) {
            throw new IllegalStateException(String.format("%d new messages, 1 expected", directedMsgs.size()));
        }
        return directedMsgs != null ? directedMsgs.get(0).getMsg() : null;
    }

    protected ByteBuf allocate(ChannelHandlerContext ctx, T msg, ByteBuf buffer) {
        // Resolve writer
        PgsqlMessageWriter<T> writer = getWriter(ctx, msg.getClass());
        if (writer == null) {
            // Should not occur
            LOGGER.error("Unsupported allocating buffer for {} message", msg.getClass().getSimpleName());
            throw new UnsupportedOperationException(
                    String.format("Unsupported allocating buffer for %s message", msg.getClass().getSimpleName()));
        }
        // Allocate buffer
        return writer.allocate(msg, buffer);
    }

    protected ByteBuf encode(ChannelHandlerContext ctx, T msg) throws IOException {
        return encode(ctx, msg, null);
    }

    protected ByteBuf encode(ChannelHandlerContext ctx, T msg, ByteBuf buffer) throws IOException {
        PgsqlMessageWriter<T> writer = getWriter(ctx, msg.getClass());
        if (writer == null) {
            // Should not occur
            LOGGER.error("Unsupported encoding of {} message", msg.getClass().getSimpleName());
            throw new UnsupportedOperationException(
                    String.format("Unsupported encoding of %s message", msg.getClass().getSimpleName()));
        }
        // Encode
        return writer.write(msg, buffer);
    }

    protected <M extends T> PgsqlMessageParser<M> getParser(ChannelHandlerContext ctx, Class<? extends T> msgType) {
        Map<Class<? extends PgsqlMessage>, PgsqlMessageParser<? extends PgsqlMessage>> map = ctx.channel()
                .attr(PgsqlConstants.MSG_PARSERS_KEY).get();
        if (map == null) {
            map = new HashMap<>();
            ctx.channel().attr(PgsqlConstants.MSG_PARSERS_KEY).set(map);
        }
        @SuppressWarnings("unchecked")
        PgsqlMessageParser<M> parser = (PgsqlMessageParser<M>) map.get(msgType);
        if (parser == null) {
            parser = buildParserWriter(msgType, true);
            map.put(msgType, parser);
        }
        return parser;
    }

    protected <M extends PgsqlMessage> PgsqlMessageWriter<M> getWriter(ChannelHandlerContext ctx,
            Class<? extends PgsqlMessage> msgType) {
        Map<Class<? extends PgsqlMessage>, PgsqlMessageWriter<? extends PgsqlMessage>> map = ctx.channel()
                .attr(PgsqlConstants.MSG_WRITERS_KEY).get();
        if (map == null) {
            map = new HashMap<>();
            ctx.channel().attr(PgsqlConstants.MSG_WRITERS_KEY).set(map);
        }
        @SuppressWarnings("unchecked")
        PgsqlMessageWriter<M> writer = (PgsqlMessageWriter<M>) map.get(msgType);
        if (writer == null) {
            writer = buildParserWriter(msgType, false);
            map.put(msgType, writer);
        }
        return writer;
    }

    @SuppressWarnings("unchecked")
    private static <WP> WP buildParserWriter(Class<? extends PgsqlMessage> msgType, boolean parser) {
        String msgTypeName = msgType.getSimpleName();
        String pkgName = PgsqlMessage.class.getPackage().getName();
        String suffix = parser ? "Parser" : "Writer";
        String className = pkgName + "." + suffix.toLowerCase() + "." + msgTypeName + suffix;
        try {
            Class<?> loadClass = PgsqlMessage.class.getClassLoader().loadClass(className);
            return (WP) loadClass.newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected int getNumberOfPeerChannels(ChannelHandlerContext ctx) {
        if (numberOfPeerChannels == 0) {
            PgsqlSession psqlSession = getPgsqlSession(ctx);
            if (ctx.channel() == psqlSession.getClientSideChannel()) {
                PgsqlConfiguration psqlConfiguration = getPsqlConfiguration(ctx);
                numberOfPeerChannels = psqlConfiguration.getServerEndpoints().size();
            } else {
                numberOfPeerChannels = 1;
            }
        }
        return numberOfPeerChannels;
    }

    protected int getPreferredPeerChannel(ChannelHandlerContext ctx) {
        if (preferredPeerChannel == Integer.MIN_VALUE) {
            PgsqlSession pgsqlSession = getPgsqlSession(ctx);
            if (ctx.channel() == pgsqlSession.getClientSideChannel()) {
                Integer preferredServerEndpoint = ctx.channel().attr(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY).get();
                if (preferredServerEndpoint == null) {
                    throw new NullPointerException(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY.name() + " is not set");
                }
                if (preferredServerEndpoint < 0
                        || preferredServerEndpoint >= pgsqlSession.getServerSideChannels().size()) {
                    throw new IndexOutOfBoundsException(
                            String.format("invalid %s: value: %d, number of server endpoints: %d ",
                                    TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY.name(), preferredServerEndpoint,
                                    pgsqlSession.getServerSideChannels().size()));
                }
                preferredPeerChannel = preferredServerEndpoint;
            } else {
                preferredPeerChannel = 0;
            }
        }
        return preferredPeerChannel;
    }

    protected PgsqlConfiguration getPsqlConfiguration(ChannelHandlerContext ctx) {
        PgsqlConfiguration pgsqlConfiguration = (PgsqlConfiguration) ctx.channel()
                .attr(PgsqlConstants.CONFIGURATION_KEY).get();
        return pgsqlConfiguration;
    }

    protected PgsqlSession getPgsqlSession(ChannelHandlerContext ctx) {
        PgsqlSession pgsqlSession = (PgsqlSession) ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        return pgsqlSession;
    }

    protected SQLSession getSqlSession(ChannelHandlerContext ctx) {
        return getPgsqlSession(ctx).getSqlSession();
    }

    protected EventProcessor getEventProcessor(ChannelHandlerContext ctx) {
        return getPgsqlSession(ctx).getEventProcessor();
    }

    protected void sendErrorResponse(ChannelHandlerContext ctx, Map<Byte, CString> errorDetails) throws IOException {
        // Build error message
        PgsqlErrorMessage msg = new PgsqlErrorMessage(errorDetails);
        // Send response
        sendResponse(ctx, msg);
    }

    protected <M extends PgsqlMessage> void sendResponse(ChannelHandlerContext ctx, M msg) throws IOException {
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

    protected <M extends PgsqlMessage> void sendRequest(ChannelHandlerContext ctx, M msg, int backend)
            throws IOException {
        // Resolve writer
        PgsqlMessageWriter<M> writer = getWriter(ctx, msg.getClass());
        // Allocate buffer
        ByteBuf buffer = writer.allocate(msg);
        // Encode
        buffer = writer.write(msg, buffer);
        // Build message
        PgsqlRawContent content = new DefaultLastPgsqlRawContent(buffer);
        if (backend == -1) {
            // Send message to all backends
            for (int i = 0; i < getPgsqlSession(ctx).getServerSideChannels().size(); i++) {
                Channel sinkChannel = getPgsqlSession(ctx).getServerSideChannel(i);
                if (i < getPgsqlSession(ctx).getServerSideChannels().size() - 1) {
                    sinkChannel.writeAndFlush(content.retainedDuplicate());
                } else {
                    sinkChannel.writeAndFlush(content);
                }
            }
        } else {
            // Send message to the specified backend
            getPgsqlSession(ctx).getServerSideChannel(backend).writeAndFlush(content);
        }
    }
}
