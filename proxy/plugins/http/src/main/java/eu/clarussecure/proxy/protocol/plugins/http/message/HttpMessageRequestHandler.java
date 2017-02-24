package eu.clarussecure.proxy.protocol.plugins.http.message;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.http.HttpConstants;
import eu.clarussecure.proxy.protocol.plugins.http.HttpSession;
import eu.clarussecure.proxy.protocol.plugins.http.message.parser.HttpMessageParser;
import eu.clarussecure.proxy.protocol.plugins.http.message.writer.HttpMessageWriter;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.spi.buffer.MutableByteBufInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;

public abstract class HttpMessageHandler<T extends HttpMessage> extends MessageToMessageDecoder<HttpObject> {

	private static final Logger LOGGER = LoggerFactory.getLogger(HttpMessageHandler.class);

	protected final Map<Byte, Class<? extends T>> msgTypes;

	@SafeVarargs
	protected HttpMessageHandler(Class<? extends T>... msgTypes) {
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
	protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
		ReferenceCountUtil.retain(msg);
		out.add(msg);
	}

	protected boolean isStreamingSupported(byte type) {
		// don't forget to configure HttpRawPartAccumulator in the pipeline
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
		HttpMessageParser<T> parser = getParser(ctx, msgType);
		// Parse content
		content.markReaderIndex();
		T msg = parser.parse(content);
		content.resetReaderIndex();
		return msg;
	}

	protected T process(ChannelHandlerContext ctx, T msg) throws IOException, NoSuchAlgorithmException {
		throw new UnsupportedOperationException(
				String.format("Unsupported processing of %s message", msg.getClass().getSimpleName()));
	}

	protected ByteBuf allocate(ChannelHandlerContext ctx, T msg, ByteBuf buffer) {
		// Resolve writer
		HttpMessageWriter<T> writer = getWriter(ctx, msg.getClass());
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
		HttpMessageWriter<T> writer = getWriter(ctx, msg.getClass());
		if (writer == null) {
			// Should not occur
			LOGGER.error("Unsupported encoding of {} message", msg.getClass().getSimpleName());
			throw new UnsupportedOperationException(
					String.format("Unsupported encoding of %s message", msg.getClass().getSimpleName()));
		}
		// Encode
		return writer.write(msg, buffer);
	}

	protected <M extends T> HttpMessageParser<M> getParser(ChannelHandlerContext ctx, Class<? extends T> msgType) {
		Map<Class<? extends HttpMessage>, HttpMessageParser<? extends HttpMessage>> map = ctx.channel()
				.attr(HttpConstants.MSG_PARSERS_KEY).get();
		if (map == null) {
			map = new HashMap<>();
			ctx.channel().attr(HttpConstants.MSG_PARSERS_KEY).set(map);
		}
		@SuppressWarnings("unchecked")
		HttpMessageParser<M> parser = (HttpMessageParser<M>) map.get(msgType);
		if (parser == null) {
			parser = buildParserWriter(msgType, true);
			map.put(msgType, parser);
		}
		return parser;
	}

	protected <M extends HttpMessage> HttpMessageWriter<M> getWriter(ChannelHandlerContext ctx,
			Class<? extends HttpMessage> msgType) {
		Map<Class<? extends HttpMessage>, HttpMessageWriter<? extends HttpMessage>> map = ctx.channel()
				.attr(HttpConstants.MSG_WRITERS_KEY).get();
		if (map == null) {
			map = new HashMap<>();
			ctx.channel().attr(HttpConstants.MSG_WRITERS_KEY).set(map);
		}
		@SuppressWarnings("unchecked")
		HttpMessageWriter<M> writer = (HttpMessageWriter<M>) map.get(msgType);
		if (writer == null) {
			writer = buildParserWriter(msgType, false);
			map.put(msgType, writer);
		}
		return writer;
	}

	@SuppressWarnings("unchecked")
	private static <WP> WP buildParserWriter(Class<? extends HttpMessage> msgType, boolean parser) {
		String msgTypeName = msgType.getSimpleName();
		String pkgName = HttpMessage.class.getPackage().getName();
		String suffix = parser ? "Parser" : "Writer";
		String className = pkgName + "." + suffix.toLowerCase() + "." + msgTypeName + suffix;
		try {
			Class<?> loadClass = HttpMessage.class.getClassLoader().loadClass(className);
			return (WP) loadClass.newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
	}

	protected HttpSession getHttpSession(ChannelHandlerContext ctx) {
		HttpSession httpSession = (HttpSession) ctx.channel().attr(TCPConstants.SESSION_KEY).get();
		return httpSession;
	}

	protected <M extends HttpMessage> void sendResponse(ChannelHandlerContext ctx, M msg) throws IOException {
		// Send message
		ctx.channel().writeAndFlush(msg);
	}

	protected <M extends HttpMessage> void sendRequest(ChannelHandlerContext ctx, M msg) throws IOException {
		// Send message
		getHttpSession(ctx).getServerSideChannel().writeAndFlush(msg);
	}
}
