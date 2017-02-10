package eu.clarussecure.proxy.protocol.plugins.http.message.ssl;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.http.HttpSession;
import eu.clarussecure.proxy.protocol.plugins.http.message.HttpSSLResponseMessage;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer;
import eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode;
import eu.clarussecure.proxy.spi.CString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class SessionInitializer {
	private static final Logger LOGGER = LoggerFactory.getLogger(SessionInitializer.class);

	private volatile SSLSessionInitializer sslSessionInitializer;
	private volatile boolean sslRequestReceived;
	private volatile boolean sslResponseReceived;
	private volatile boolean sessionEncryptedOnClientSide;
	private volatile boolean sessionEncryptedOnServerSide;

	public SessionInitializer() {
		sslSessionInitializer = new SSLSessionInitializer();
		sslRequestReceived = false;
		sslResponseReceived = false;
		sessionEncryptedOnClientSide = false;
		sessionEncryptedOnServerSide = false;
	}

	public SessionMessageTransferMode<Void, Byte> processSSLRequest(ChannelHandlerContext ctx, int code)
			throws IOException {
		LOGGER.debug("SSL request code: {}", code);
		TransferMode transferMode = TransferMode.FORWARD;
		Byte response = null;
		Map<Byte, CString> errorDetails = null;
		sslRequestReceived = true;
		if (sslSessionInitializer.getClientMode() == SSLMode.DISABLED) {
			// Client side: reply SSL is disabled
			LOGGER.trace("Reply to the client that SSL is required");
			transferMode = TransferMode.ERROR;
			errorDetails = new LinkedHashMap<>();
			errorDetails.put((byte) 'S', CString.valueOf("FATAL"));
			errorDetails.put((byte) 'M', CString.valueOf("SSL is disabled"));
			// Server side: don't forward message to the server
			LOGGER.trace("SSL request is ignored (due to an error on the client side)");
		} else {
			LOGGER.trace("SSL is allowed or required on the client side");
			if (sslSessionInitializer.getServerMode() == SSLMode.DISABLED) {
				// Client side: initialize and add SSL handler in client
				// pipeline
				addSSLHandlerOnClientSide(ctx);
				// Reply SSL is ok
				LOGGER.trace("Reply SSL to the client");
				transferMode = TransferMode.FORGET;
				response = HttpSSLResponseMessage.CODE_SSL;
				// Server side: don't forward message to the server
				LOGGER.trace("SSL request is ignored (SSL is disabled on the server side)");
			} else {
				// Server side: forward message to the server
				LOGGER.trace("Forward the SSL request (SSL is allowed or required on the server side)");
			}
		}
		SessionMessageTransferMode<Void, Byte> mode = new SessionMessageTransferMode<Void, Byte>(null, transferMode,
				response, errorDetails);
		LOGGER.debug("SSL request processed: transfer mode={}", mode);
		return mode;
	}

	public SessionMessageTransferMode<Byte, Void> processSSLResponse(ChannelHandlerContext ctx, byte code)
			throws IOException {
		LOGGER.debug("SSL response code: {}", code);
		TransferMode transferMode = TransferMode.FORWARD;
		byte newCode = code;
		sslResponseReceived = true;
		if (code == HttpSSLResponseMessage.CODE_SSL) {
			if (sslRequestReceived) {
				if (sslSessionInitializer.getClientMode() == SSLMode.DISABLED) {
					LOGGER.trace("SSL is disabled on the client side");
					// Client side: modify SSL code
					LOGGER.trace("Modify SSL code to NO_SSL");
					newCode = HttpSSLResponseMessage.CODE_NO_SSL;
				} else {
					// Client side: forward message to the client
					LOGGER.trace("Forward the SSL response (SSL was required by the client)");
					// Client side: initialize and add SSL handler in client
					// pipeline
					addSSLHandlerOnClientSide(ctx);
				}
			} else {
				// Client side: don't forward message to the client
				LOGGER.trace("SSL response is ignored (client did not request SSL)");
				transferMode = TransferMode.FORGET;
				// Server side: remove the SSLInitializationHandler
				removeSessionInitializationResponseHandler(ctx);
			}
			// Initialize and add SSL handler in server pipeline
			addSSLHandlerOnServerSide(ctx);
		} else if (code == HttpSSLResponseMessage.CODE_NO_SSL) {
			if (sslRequestReceived) {
				// Client side: forward message to the client
				LOGGER.trace("Forward the SSL response (SSL was required by the client)");
			} else {
				// Client side: don't forward message to the client
				LOGGER.trace("SSL response is ignored (client did not request SSL)");
				transferMode = TransferMode.FORGET;
				// Server side: remove the SSLInitializationHandler
				removeSessionInitializationResponseHandler(ctx);
			}
		}
		// Notify other threads that response was received
		synchronized (this) {
			notifyAll();
		}
		SessionMessageTransferMode<Byte, Void> mode = new SessionMessageTransferMode<>(newCode, transferMode);
		LOGGER.debug("SSL response processed: new code={}, transfer mode={}", newCode, mode);
		return mode;
	}

	public SessionMessageTransferMode<Void, Void> processStartupMessage(ChannelHandlerContext ctx) throws IOException {
		LOGGER.debug("Start-up message");
		TransferMode transferMode = TransferMode.FORWARD;
		Map<Byte, CString> errorDetails = null;
		if (sslRequestReceived) {
			LOGGER.trace("Session initialization completed");
			// Client side: nothing todo
			LOGGER.trace("Session {} on the client side",
					sessionEncryptedOnClientSide ? "encrypted with SSL" : "not encrypted");
			// Server side: nothing todo
			LOGGER.trace("Session {} on the server side",
					sessionEncryptedOnServerSide ? "encrypted with SSL" : "not encrypted");
		} else {
			if (sslSessionInitializer.getClientMode() == SSLMode.REQUIRED) {
				// Client side: reply SSL is required
				LOGGER.trace("Reply to the client that SSL is required");
				transferMode = TransferMode.ERROR;
				errorDetails = new LinkedHashMap<>();
				errorDetails.put((byte) 'S', CString.valueOf("FATAL"));
				errorDetails.put((byte) 'M', CString.valueOf("SSL is required"));
				// Server side: don't forward message to the server
				LOGGER.trace("SSL request is ignored (due to an error on the client side)");
			} else {
				// Server side: sent SSL request if SSL is required
				if (sslSessionInitializer.getServerMode() == SSLMode.REQUIRED) {
					LOGGER.trace("Handle SSL initialization with the server");
					transferMode = TransferMode.ORCHESTRATE;
				} else {
					LOGGER.trace("Session initialization completed");
					// Client side: nothing todo
					LOGGER.trace("Session {} on the client side",
							sessionEncryptedOnClientSide ? "encrypted with SSL" : "not encrypted");
					// Server side: nothing todo
					LOGGER.trace("Session {} on the server side",
							sessionEncryptedOnServerSide ? "encrypted with SSL" : "not encrypted");
				}
			}
		}
		// Remove SSLInitializationHandler on the client side
		removeSessionInitializationRequestHandler(ctx);
		if (transferMode != TransferMode.ORCHESTRATE) {
			// Remove SSLInitializationHandler on the server side
			removeSessionInitializationResponseHandler(ctx);
			// Configure HttpRawPartCodec to skip SSL response on the server
			skipSSLResponse(ctx);
		}
		SessionMessageTransferMode<Void, Void> mode = new SessionMessageTransferMode<>(null, transferMode,
				errorDetails);
		LOGGER.debug("Start-up message processed: transfer mode={}", mode);
		return mode;
	}

	public void waitForResponse() throws IOException {
		while (!sslResponseReceived) {
			synchronized (this) {
				try {
					wait();
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
			}
		}
	}

	public void addSSLHandlerOnClientSide(ChannelHandlerContext ctx) throws IOException {
		Future<Channel> handshakeFuture = sslSessionInitializer.addSSLHandlerOnClientSide(ctx,
				getHttpSession(ctx).getClientSideChannel().pipeline());
		handshakeFuture.addListener(new GenericFutureListener<Future<? super Channel>>() {

			@Override
			public void operationComplete(Future<? super Channel> future) throws Exception {
				sessionEncryptedOnClientSide = true;
				LOGGER.trace("SSL handshake for client side completed");
			}
		});
	}

	public void addSSLHandlerOnServerSide(ChannelHandlerContext ctx) throws SSLException {
		Future<Channel> handshakeFuture = sslSessionInitializer.addSSLHandlerOnServerSide(ctx);
		handshakeFuture.addListener(new GenericFutureListener<Future<? super Channel>>() {

			@Override
			public void operationComplete(Future<? super Channel> future) throws Exception {
				sessionEncryptedOnServerSide = true;
				LOGGER.trace("SSL handshake for server side completed");
			}
		});
	}

	private void removeSessionInitializationRequestHandler(ChannelHandlerContext ctx) {
		ChannelPipeline pipeline = ctx.pipeline();
		ChannelHandler handler = pipeline.get("SessionInitializationRequestHandler");
		if (handler != null) {
			pipeline.remove(handler);
		}
	}

	private void removeSessionInitializationResponseHandler(ChannelHandlerContext ctx) {
		ChannelPipeline pipeline = getHttpSession(ctx).getServerSideChannel().pipeline();
		ChannelHandler handler = pipeline.get("SessionInitializationResponseHandler");
		if (handler != null) {
			pipeline.remove(handler);
		}
	}

	private void skipSSLResponse(ChannelHandlerContext ctx) {
		ChannelPipeline pipeline = getHttpSession(ctx).getServerSideChannel().pipeline();
		HttpClientCodec codec = (HttpClientCodec) pipeline.get("HttpClientCodec");
		//codec.skipFirstMessages();
	}

	private HttpSession getHttpSession(ChannelHandlerContext ctx) {
		HttpSession httpSession = (HttpSession) ctx.channel().attr(TCPConstants.SESSION_KEY).get();
		return httpSession;
	}

}
