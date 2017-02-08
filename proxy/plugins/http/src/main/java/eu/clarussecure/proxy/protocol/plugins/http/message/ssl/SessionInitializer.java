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
	private volatile boolean sessionEncryptedOnFrontendSide;
	private volatile boolean sessionEncryptedOnBackendSide;

	public SessionInitializer() {
		sslSessionInitializer = new SSLSessionInitializer();
		sslRequestReceived = false;
		sslResponseReceived = false;
		sessionEncryptedOnFrontendSide = false;
		sessionEncryptedOnBackendSide = false;
	}

	public SessionMessageTransferMode<Void, Byte> processSSLRequest(ChannelHandlerContext ctx, int code)
			throws IOException {
		LOGGER.debug("SSL request code: {}", code);
		TransferMode transferMode = TransferMode.FORWARD;
		Byte response = null;
		Map<Byte, CString> errorDetails = null;
		sslRequestReceived = true;
		if (sslSessionInitializer.getClientMode() == SSLMode.DISABLED) {
			// Frontend side: reply SSL is disabled
			LOGGER.trace("Reply to the frontend that SSL is required");
			transferMode = TransferMode.ERROR;
			errorDetails = new LinkedHashMap<>();
			errorDetails.put((byte) 'S', CString.valueOf("FATAL"));
			errorDetails.put((byte) 'M', CString.valueOf("SSL is disabled"));
			// Backend side: don't forward message to the backend
			LOGGER.trace("SSL request is ignored (due to an error on the frontend side)");
		} else {
			LOGGER.trace("SSL is allowed or required on the frontend side");
			if (sslSessionInitializer.getServerMode() == SSLMode.DISABLED) {
				// Frontend side: initialize and add SSL handler in frontend
				// pipeline
				addSSLHandlerOnFrontendSide(ctx);
				// Reply SSL is ok
				LOGGER.trace("Reply SSL to the frontend");
				transferMode = TransferMode.FORGET;
				response = HttpSSLResponseMessage.CODE_SSL;
				// Backend side: don't forward message to the backend
				LOGGER.trace("SSL request is ignored (SSL is disabled on the backend side)");
			} else {
				// Backend side: forward message to the backend
				LOGGER.trace("Forward the SSL request (SSL is allowed or required on the backend side)");
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
					LOGGER.trace("SSL is disabled on the frontend side");
					// Frontend side: modify SSL code
					LOGGER.trace("Modify SSL code to NO_SSL");
					newCode = HttpSSLResponseMessage.CODE_NO_SSL;
				} else {
					// Frontend side: forward message to the frontend
					LOGGER.trace("Forward the SSL response (SSL was required by the frontend)");
					// Frontend side: initialize and add SSL handler in frontend
					// pipeline
					addSSLHandlerOnFrontendSide(ctx);
				}
			} else {
				// Frontend side: don't forward message to the frontend
				LOGGER.trace("SSL response is ignored (frontend did not request SSL)");
				transferMode = TransferMode.FORGET;
				// Backend side: remove the SSLInitializationHandler
				removeSessionInitializationResponseHandler(ctx);
			}
			// Initialize and add SSL handler in backend pipeline
			addSSLHandlerOnBackendSide(ctx);
		} else if (code == HttpSSLResponseMessage.CODE_NO_SSL) {
			if (sslRequestReceived) {
				// Frontend side: forward message to the frontend
				LOGGER.trace("Forward the SSL response (SSL was required by the frontend)");
			} else {
				// Frontend side: don't forward message to the frontend
				LOGGER.trace("SSL response is ignored (frontend did not request SSL)");
				transferMode = TransferMode.FORGET;
				// Backend side: remove the SSLInitializationHandler
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
			// Frontend side: nothing todo
			LOGGER.trace("Session {} on the frontend side",
					sessionEncryptedOnFrontendSide ? "encrypted with SSL" : "not encrypted");
			// Backend side: nothing todo
			LOGGER.trace("Session {} on the backend side",
					sessionEncryptedOnBackendSide ? "encrypted with SSL" : "not encrypted");
		} else {
			if (sslSessionInitializer.getClientMode() == SSLMode.REQUIRED) {
				// Frontend side: reply SSL is required
				LOGGER.trace("Reply to the frontend that SSL is required");
				transferMode = TransferMode.ERROR;
				errorDetails = new LinkedHashMap<>();
				errorDetails.put((byte) 'S', CString.valueOf("FATAL"));
				errorDetails.put((byte) 'M', CString.valueOf("SSL is required"));
				// Backend side: don't forward message to the backend
				LOGGER.trace("SSL request is ignored (due to an error on the frontend side)");
			} else {
				// Backend side: sent SSL request if SSL is required
				if (sslSessionInitializer.getServerMode() == SSLMode.REQUIRED) {
					LOGGER.trace("Handle SSL initialization with the backend");
					transferMode = TransferMode.ORCHESTRATE;
				} else {
					LOGGER.trace("Session initialization completed");
					// Frontend side: nothing todo
					LOGGER.trace("Session {} on the frontend side",
							sessionEncryptedOnFrontendSide ? "encrypted with SSL" : "not encrypted");
					// Backend side: nothing todo
					LOGGER.trace("Session {} on the backend side",
							sessionEncryptedOnBackendSide ? "encrypted with SSL" : "not encrypted");
				}
			}
		}
		// Remove SSLInitializationHandler on the frontend side
		removeSessionInitializationRequestHandler(ctx);
		if (transferMode != TransferMode.ORCHESTRATE) {
			// Remove SSLInitializationHandler on the backend side
			removeSessionInitializationResponseHandler(ctx);
			// Configure HttpRawPartCodec to skip SSL response on the backend
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

	private void addSSLHandlerOnFrontendSide(ChannelHandlerContext ctx) throws IOException {
		Future<Channel> handshakeFuture = sslSessionInitializer.addSSLHandlerOnClientSide(ctx,
				getPsqlSession(ctx).getClientSideChannel().pipeline());
		handshakeFuture.addListener(new GenericFutureListener<Future<? super Channel>>() {

			@Override
			public void operationComplete(Future<? super Channel> future) throws Exception {
				sessionEncryptedOnFrontendSide = true;
				LOGGER.trace("SSL handshake for frontend side completed");
			}
		});
	}

	private void addSSLHandlerOnBackendSide(ChannelHandlerContext ctx) throws SSLException {
		Future<Channel> handshakeFuture = sslSessionInitializer.addSSLHandlerOnServerSide(ctx);
		handshakeFuture.addListener(new GenericFutureListener<Future<? super Channel>>() {

			@Override
			public void operationComplete(Future<? super Channel> future) throws Exception {
				sessionEncryptedOnBackendSide = true;
				LOGGER.trace("SSL handshake for backend side completed");
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
		ChannelPipeline pipeline = getPsqlSession(ctx).getServerSideChannel().pipeline();
		ChannelHandler handler = pipeline.get("SessionInitializationResponseHandler");
		if (handler != null) {
			pipeline.remove(handler);
		}
	}

	private void skipSSLResponse(ChannelHandlerContext ctx) {
		ChannelPipeline pipeline = getPsqlSession(ctx).getServerSideChannel().pipeline();
		HttpClientCodec codec = (HttpClientCodec) pipeline.get("HttpClientCodec");
		// codec.skipFirstMessages();
	}

	private HttpSession getPsqlSession(ChannelHandlerContext ctx) {
		HttpSession pgsqlSession = (HttpSession) ctx.channel().attr(TCPConstants.SESSION_KEY).get();
		return pgsqlSession;
	}

}
