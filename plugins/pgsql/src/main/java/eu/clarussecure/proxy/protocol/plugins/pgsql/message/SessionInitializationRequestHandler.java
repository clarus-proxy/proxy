package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.ssl.SessionInitializer;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.ssl.SessionMessageTransferMode;
import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.DirectedMessage;
import io.netty.channel.ChannelHandlerContext;

public class SessionInitializationRequestHandler extends PgsqlMessageHandler<PgsqlSessionInitializationRequestMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionInitializationRequestHandler.class);

    public SessionInitializationRequestHandler() {
        super(PgsqlSSLRequestMessage.class, PgsqlStartupMessage.class, PgsqlCancelRequestMessage.class);
    }

    @Override
    protected List<DirectedMessage<PgsqlSessionInitializationRequestMessage>> directedProcess(ChannelHandlerContext ctx,
            PgsqlSessionInitializationRequestMessage msg) throws IOException {
        PgsqlSessionInitializationRequestMessage newMsg = process(ctx, msg);
        if (newMsg == null) {
            return null;
        }
        // forward the same message to all channels
        DirectedMessage<PgsqlSessionInitializationRequestMessage> directedMsg = new DirectedMessage<>(-1, newMsg);
        List<DirectedMessage<PgsqlSessionInitializationRequestMessage>> directedMsgs = Collections
                .singletonList(directedMsg);
        return directedMsgs;
    }

    @Override
    protected PgsqlSessionInitializationRequestMessage process(ChannelHandlerContext ctx,
            PgsqlSessionInitializationRequestMessage msg) throws IOException {
        PgsqlSessionInitializationRequestMessage newMsg = msg;
        if (msg instanceof PgsqlSSLRequestMessage) {
            int code = ((PgsqlSSLRequestMessage) msg).getCode();
            LOGGER.debug("SSL request: {}", code);
            SessionMessageTransferMode<Void, Byte> transferMode = getSessionInitializer(ctx).processSSLRequest(ctx,
                    code);
            switch (transferMode.getTransferMode()) {
            case FORWARD:
                // Forward the message
                LOGGER.trace("Forward the SSL request");
                break;
            case FORGET:
                // Reply if necessary
                if (transferMode.getResponse() != null) {
                    LOGGER.trace("Send the SSL response");
                    // Send SSL response to the frontend
                    sendSSLResponse(ctx, transferMode.getResponse());
                }
                // Don't forward the message
                LOGGER.trace("Ignore the SSL request");
                newMsg = null;
                break;
            case ERROR:
                // Send error message to the frontend
                LOGGER.trace("Send an error response");
                sendErrorResponse(ctx, transferMode.getErrorDetails());
                newMsg = null;
                break;
            case ORCHESTRATE:
            default:
                // Should not occur
                throw new IllegalArgumentException(
                        "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": "
                                + transferMode.getTransferMode());
            }
        } else if (msg instanceof PgsqlStartupMessage) {
            SessionMessageTransferMode<Void, Void> transferMode = getSessionInitializer(ctx).processStartupMessage(ctx);
            switch (transferMode.getTransferMode()) {
            case FORWARD:
                // Forward the message
                LOGGER.trace("Forward the start-up message");
                break;
            case ORCHESTRATE:
                // 1: send SSL request to the backend
                LOGGER.trace("Send the SSL request");
                sendSSLRequest(ctx, PgsqlSSLRequestMessage.CODE);
                // 2: wait for the SSL response
                LOGGER.trace("Wait for the SSL response");
                waitForResponses(ctx);
                // 3: forward the start-up message
                LOGGER.trace("Forward the start-up message");
                break;
            case ERROR:
                // Send error message to the frontend
                LOGGER.trace("Send an error response");
                sendErrorResponse(ctx, transferMode.getErrorDetails());
                newMsg = null;
                break;
            case FORGET:
            default:
                // Should not occur
                throw new IllegalArgumentException(
                        "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": "
                                + transferMode.getTransferMode());
            }
        } else if (msg instanceof PgsqlCancelRequestMessage) {
            int code = ((PgsqlCancelRequestMessage) msg).getCode();
            int processId = ((PgsqlCancelRequestMessage) msg).getProcessId();
            int secretKey = ((PgsqlCancelRequestMessage) msg).getSecretKey();
            LOGGER.debug("Cancel request: code={}, process ID={}, secret key={}", code, processId, secretKey);
            SessionMessageTransferMode<Void, Void> transferMode = getSessionInitializer(ctx).processCancelRequest(ctx,
                    code, processId, secretKey);
            switch (transferMode.getTransferMode()) {
            case FORWARD:
                // Forward the message
                LOGGER.trace("Forward the cancel request");
                break;
            case FORGET:
                // Don't forward the message
                LOGGER.trace("Ignore the cancel request");
                newMsg = null;
                break;
            case ERROR:
            case ORCHESTRATE:
            default:
                // Should not occur
                throw new IllegalArgumentException(
                        "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": "
                                + transferMode.getTransferMode());
            }
        }
        return newMsg;
    }

    private void sendSSLRequest(ChannelHandlerContext ctx, int code) throws IOException {
        // Build SSL request message
        PgsqlSSLRequestMessage msg = new PgsqlSSLRequestMessage(code);
        // Send request to all backends
        sendRequest(ctx, msg, -1);
    }

    private void sendSSLResponse(ChannelHandlerContext ctx, byte code) throws IOException {
        // Build SSL response message
        PgsqlSSLResponseMessage msg = new PgsqlSSLResponseMessage(code);
        // Send response
        sendResponse(ctx, msg);
    }

    private void waitForResponses(ChannelHandlerContext ctx) throws IOException {
        // Wait for responses
        SessionInitializer sessionInitializer = getSessionInitializer(ctx);
        sessionInitializer.waitForResponses(ctx);
    }

    private SessionInitializer getSessionInitializer(ChannelHandlerContext ctx) {
        return getPgsqlSession(ctx).getSessionInitializer();
    }

}
