package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.ssl.SessionInitializer;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.ssl.SessionMessageTransferMode;
import io.netty.channel.ChannelHandlerContext;

public class SessionInitializationResponseHandler extends PgsqlMessageHandler<PgsqlSessionInitializationResponseMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionInitializationResponseHandler.class);

    public SessionInitializationResponseHandler() {
        super(PgsqlSSLResponseMessage.class);
    }

    @Override
    protected PgsqlSessionInitializationResponseMessage process(ChannelHandlerContext ctx, PgsqlSessionInitializationResponseMessage msg) throws IOException {
        PgsqlSessionInitializationResponseMessage newMsg = msg;
        if (msg instanceof PgsqlSSLResponseMessage) {
            byte code = ((PgsqlSSLResponseMessage) msg).getCode();
            LOGGER.debug("SSL response: {}", code);
            SessionMessageTransferMode<Byte, Void> transferMode = getSessionInitializer(ctx).processSSLResponse(ctx, ((PgsqlSSLResponseMessage) msg).getCode());
            
            switch (transferMode.getTransferMode()) {
            case FORWARD:
                // Forward the message
                if (transferMode.getNewDetails() != code) {
                    LOGGER.trace("Modify the SSL response");
                    newMsg = new PgsqlSSLResponseMessage(transferMode.getNewDetails());
                }
                LOGGER.trace("Forward the SSL response");
                break;
            case FORGET:
                // Don't forward the message
                LOGGER.trace("Ignore the SSL response");
                newMsg = null;
                break;
            case ERROR:
            case ORCHESTRATE:
            default:
                // Should not occur
                throw new IllegalArgumentException( "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": " + transferMode.getTransferMode());
            }
        }
        return newMsg;
    }

    private SessionInitializer getSessionInitializer(ChannelHandlerContext ctx) {
        return getPsqlSession(ctx).getSessionInitializer();
    }

}
