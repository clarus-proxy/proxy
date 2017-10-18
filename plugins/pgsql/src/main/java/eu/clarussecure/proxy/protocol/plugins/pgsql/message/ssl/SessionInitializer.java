package eu.clarussecure.proxy.protocol.plugins.pgsql.message.ssl;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConstants;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSSLResponseMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.TransferMode;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartCodec;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer;
import eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode;
import eu.clarussecure.proxy.spi.CString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class SessionInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionInitializer.class);

    private SSLSessionInitializer sslSessionInitializer;
    private AtomicBoolean sslRequestReceived;
    private AtomicInteger sslResponseReceived;
    private AtomicBoolean sessionEncryptedOnFrontendSide;
    private AtomicBoolean sessionEncryptedOnBackendSide;

    public SessionInitializer() {
        sslSessionInitializer = new SSLSessionInitializer();
        sslRequestReceived = new AtomicBoolean(false);
        sslResponseReceived = new AtomicInteger(0);
        sessionEncryptedOnFrontendSide = new AtomicBoolean(false);
        sessionEncryptedOnBackendSide = new AtomicBoolean(false);
    }

    public SessionMessageTransferMode<Void, Byte> processSSLRequest(ChannelHandlerContext ctx, int code)
            throws IOException {
        LOGGER.debug("SSL request code: {}", code);
        TransferMode transferMode = TransferMode.FORWARD;
        Byte response = null;
        Map<Byte, CString> errorDetails = null;
        sslRequestReceived.set(true);
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
                // Frontend side: initialize and add SSL handler in frontend pipeline
                addSSLHandlerOnFrontendSide(ctx);
                // Reply SSL is ok
                LOGGER.trace("Reply SSL to the frontend");
                transferMode = TransferMode.FORGET;
                response = PgsqlSSLResponseMessage.CODE_SSL;
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
        Byte newCode = code;
        if (code == PgsqlSSLResponseMessage.CODE_SSL) {
            if (sslRequestReceived.get()) {
                if (sslSessionInitializer.getClientMode() == SSLMode.DISABLED) {
                    LOGGER.trace("SSL is disabled on the frontend side");
                    // Frontend side: modify SSL code
                    LOGGER.trace("Modify SSL code to NO_SSL");
                    newCode = PgsqlSSLResponseMessage.CODE_NO_SSL;
                } else {
                    // Forget all responses except the one from the preferred server
                    int serverEndpoint = getServerEndPoint(ctx);
                    int preferredServerEndpoint = getPreferredServerEndPoint(ctx);
                    if (serverEndpoint != preferredServerEndpoint) {
                        transferMode = TransferMode.FORGET;
                        newCode = null;
                    } else {
                        // Frontend side: forward message to the frontend
                        LOGGER.trace("Forward the SSL response (SSL was required by the frontend)");
                        // Frontend side: initialize and add SSL handler in frontend pipeline
                        addSSLHandlerOnFrontendSide(ctx);
                    }
                }
            } else {
                // Frontend side: don't forward message to the frontend
                LOGGER.trace("SSL response is ignored (frontend did not request SSL)");
                transferMode = TransferMode.FORGET;
                newCode = null;
                // Backend side: remove the SSLInitializationHandler
                removeSessionInitializationResponseHandler(ctx, false);
            }
            // Initialize and add SSL handler in backend pipeline
            addSSLHandlerOnBackendSide(ctx);
        } else if (code == PgsqlSSLResponseMessage.CODE_NO_SSL) {
            if (sslRequestReceived.get()) {
                // Forget all responses except the one from the preferred server
                int serverEndpoint = getServerEndPoint(ctx);
                int preferredServerEndpoint = getPreferredServerEndPoint(ctx);
                if (serverEndpoint != preferredServerEndpoint) {
                    transferMode = TransferMode.FORGET;
                    newCode = null;
                } else {
                    // Frontend side: forward message to the frontend
                    LOGGER.trace("Forward the SSL response (SSL was required by the frontend)");
                }
            } else {
                // Frontend side: don't forward message to the frontend
                LOGGER.trace("SSL response is ignored (frontend did not request SSL)");
                transferMode = TransferMode.FORGET;
                newCode = null;
                // Backend side: remove the SSLInitializationHandler
                removeSessionInitializationResponseHandler(ctx, false);
            }
        }
        // Notify other threads that response was received
        synchronized (this) {
            sslResponseReceived.incrementAndGet();
            notifyAll();
        }
        SessionMessageTransferMode<Byte, Void> mode = new SessionMessageTransferMode<>(newCode, transferMode);
        LOGGER.debug("SSL response processed: new code={}, transfer mode={}", newCode, mode);
        return mode;
    }

    private int getServerEndPoint(ChannelHandlerContext ctx) {
        Integer serverEndpointNumber = ctx.channel().attr(TCPConstants.SERVER_ENDPOINT_NUMBER_KEY).get();
        if (serverEndpointNumber == null) {
            throw new NullPointerException(TCPConstants.SERVER_ENDPOINT_NUMBER_KEY.name() + " is not set");
        }
        PgsqlSession pgsqlSession = getPgsqlSession(ctx);
        if (serverEndpointNumber < 0 || serverEndpointNumber >= pgsqlSession.getServerSideChannels().size()) {
            throw new IndexOutOfBoundsException(String.format("invalid %s: value: %d, number of server endpoints: %d ",
                    TCPConstants.SERVER_ENDPOINT_NUMBER_KEY.name(), serverEndpointNumber,
                    pgsqlSession.getServerSideChannels().size()));
        }
        return serverEndpointNumber;
    }

    private int getPreferredServerEndPoint(ChannelHandlerContext ctx) {
        Integer preferredServerEndpoint = ctx.channel().attr(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY).get();
        if (preferredServerEndpoint == null) {
            throw new NullPointerException(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY.name() + " is not set");
        }
        PgsqlSession pgsqlSession = getPgsqlSession(ctx);
        if (preferredServerEndpoint < 0 || preferredServerEndpoint >= pgsqlSession.getServerSideChannels().size()) {
            throw new IndexOutOfBoundsException(String.format("invalid %s: value: %d, number of server endpoints: %d ",
                    TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY.name(), preferredServerEndpoint,
                    pgsqlSession.getServerSideChannels().size()));
        }
        return preferredServerEndpoint;
    }

    public SessionMessageTransferMode<Void, Void> processStartupMessage(ChannelHandlerContext ctx) throws IOException {
        LOGGER.debug("Start-up message");
        TransferMode transferMode = TransferMode.FORWARD;
        Map<Byte, CString> errorDetails = null;
        if (sslRequestReceived.get()) {
            LOGGER.trace("Session initialization completed");
            // Frontend side: nothing todo
            LOGGER.trace("Session {} on the frontend side",
                    sessionEncryptedOnFrontendSide.get() ? "encrypted with SSL" : "not encrypted");
            // Backend side: nothing todo
            LOGGER.trace("Session {} on the backend side",
                    sessionEncryptedOnBackendSide.get() ? "encrypted with SSL" : "not encrypted");
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
                            sessionEncryptedOnFrontendSide.get() ? "encrypted with SSL" : "not encrypted");
                    // Backend side: nothing todo
                    LOGGER.trace("Session {} on the backend side",
                            sessionEncryptedOnBackendSide.get() ? "encrypted with SSL" : "not encrypted");
                }
            }
        }
        // Remove SSLInitializationHandler on the frontend side
        removeSessionInitializationRequestHandler(ctx);
        if (transferMode != TransferMode.ORCHESTRATE) {
            // Remove SSLInitializationHandler on the backend side
            removeSessionInitializationResponseHandler(ctx, true);
            // Configure PgsqlRawPartCodec to skip SSL response on the backend
            skipSSLResponse(ctx);
        }
        SessionMessageTransferMode<Void, Void> mode = new SessionMessageTransferMode<>(null, transferMode,
                errorDetails);
        LOGGER.debug("Start-up message processed: transfer mode={}", mode);
        return mode;
    }

    public void waitForResponses(ChannelHandlerContext ctx) throws IOException {
        PgsqlSession pgsqlSession = getPgsqlSession(ctx);
        synchronized (this) {
            while (sslResponseReceived.get() < pgsqlSession.getServerSideChannels().size()) {
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
                getPgsqlSession(ctx).getClientSideChannel().pipeline());
        handshakeFuture.addListener(new GenericFutureListener<Future<? super Channel>>() {

            @Override
            public void operationComplete(Future<? super Channel> future) throws Exception {
                sessionEncryptedOnFrontendSide.set(true);
                LOGGER.trace("SSL handshake for frontend side completed");
            }
        });
    }

    private void addSSLHandlerOnBackendSide(ChannelHandlerContext ctx) throws SSLException {
        Future<Channel> handshakeFuture = sslSessionInitializer.addSSLHandlerOnServerSide(ctx);
        handshakeFuture.addListener(new GenericFutureListener<Future<? super Channel>>() {

            @Override
            public void operationComplete(Future<? super Channel> future) throws Exception {
                sessionEncryptedOnBackendSide.set(true);
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

    private void removeSessionInitializationResponseHandler(ChannelHandlerContext ctx, boolean all) {
        List<Channel> serverSideChannels;
        if (all) {
            serverSideChannels = getPgsqlSession(ctx).getServerSideChannels();
        } else {
            int serverEndPoint = getServerEndPoint(ctx);
            Channel serverSideChannel = getPgsqlSession(ctx).getServerSideChannel(serverEndPoint);
            serverSideChannels = Collections.singletonList(serverSideChannel);
        }
        for (Channel serverSideChannel : serverSideChannels) {
            ChannelPipeline pipeline = serverSideChannel.pipeline();
            ChannelHandler handler = pipeline.get("SessionInitializationResponseHandler");
            if (handler != null) {
                pipeline.remove(handler);
            }
        }
    }

    private void skipSSLResponse(ChannelHandlerContext ctx) {
        for (Channel serverSideChannel : getPgsqlSession(ctx).getServerSideChannels()) {
            ChannelPipeline pipeline = serverSideChannel.pipeline();
            PgsqlRawPartCodec codec = (PgsqlRawPartCodec) pipeline.get("PgsqlPartCodec");
            codec.skipFirstMessages();
        }
    }

    public SessionMessageTransferMode<Void, Void> processCancelRequest(ChannelHandlerContext ctx, int code,
            int processId, int secretKey) throws IOException {
        LOGGER.debug("Cancel request: code={}, process ID={}, secret key={}", code, processId, secretKey);
        TransferMode transferMode = TransferMode.FORWARD;
        Map<Byte, CString> errorDetails = null;
        // Backend side: forward message to the backend
        LOGGER.trace("Forward the cancel request");
        SessionMessageTransferMode<Void, Void> mode = new SessionMessageTransferMode<Void, Void>(null, transferMode,
                errorDetails);
        LOGGER.debug("Cancel request processed: transfer mode={}", mode);
        return mode;
    }

    private PgsqlSession getPgsqlSession(ChannelHandlerContext ctx) {
        PgsqlSession pgsqlSession = (PgsqlSession) ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        return pgsqlSession;
    }

}
