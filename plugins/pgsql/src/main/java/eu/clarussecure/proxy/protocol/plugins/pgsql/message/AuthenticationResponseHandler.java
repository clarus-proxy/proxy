package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.AuthenticationResponse;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.MessageTransferMode;
import io.netty.channel.ChannelHandlerContext;

public class AuthenticationResponseHandler extends PgsqlMessageHandler<PgsqlAuthenticationResponse> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationResponseHandler.class);

    public AuthenticationResponseHandler() {
        super(PgsqlAuthenticationResponse.class);
    }

    @Override
    protected PgsqlAuthenticationResponse process(ChannelHandlerContext ctx, PgsqlAuthenticationResponse msg)
            throws IOException {
        LOGGER.debug("Authentication Type : {}", msg.getAuthenticationType());
        PgsqlAuthenticationResponse newMsg = msg;
        // Process authentication request
        AuthenticationResponse request = new AuthenticationResponse(msg.getAuthenticationType(),
                msg.getAuthenticationParameters());
        MessageTransferMode<AuthenticationResponse, Void> transferMode = getEventProcessor(ctx)
                .processAuthenticationResponse(ctx, request);
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            AuthenticationResponse newRequest = transferMode.getNewContent();
            if (newRequest != request) {
                newMsg = new PgsqlAuthenticationResponse(newRequest.getType(), newRequest.getParameters());
                LOGGER.trace("AuthenticationResponse modified: original was: {}", msg);
                LOGGER.trace("AuthenticationResponse modified: new is : {}", newMsg);
            }
            break;
        case FORGET:
            newMsg = null;
            LOGGER.trace("AuthenticationResponse dropped");
            break;
        default:
            // Should not occur
            throw new IllegalArgumentException(
                    "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": "
                            + transferMode.getTransferMode());
        }
        return newMsg;
    }

}
