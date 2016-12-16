package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.spi.CString;
import io.netty.channel.ChannelHandlerContext;

public class AuthenticationHandler extends PgsqlMessageHandler<PgsqlAuthenticationMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationHandler.class);

    public AuthenticationHandler() {
        super(PgsqlStartupMessage.class, PgsqlAuthenticationCleartextPasswordMessage.class, PgsqlAuthenticationOkMessage.class, PgsqlPasswordMessage.class);
    }

    @Override
    protected PgsqlAuthenticationMessage process(ChannelHandlerContext ctx, PgsqlAuthenticationMessage msg) throws IOException {
        if (msg instanceof PgsqlStartupMessage) {
            PgsqlStartupMessage msgStartup = (PgsqlStartupMessage) msg;
            LOGGER.debug("User id: {}, database: {}", msgStartup.getParameters().get(CString.valueOf("user")), msgStartup.getParameters().get(CString.valueOf("database")));
            PgsqlStartupMessage newMsg = msgStartup; 
            CString userIdentificate = getEventProcessor(ctx).processAuthentication(ctx, msgStartup.getParameters());
            CString userMsg = msgStartup.getParameters().get(CString.valueOf("user"));
            if(!userIdentificate.equals(userMsg)){
                newMsg = new PgsqlStartupMessage(msgStartup.getProtocolVersion(), msgStartup.getParameters());
                Map<CString, CString> newMsgParameters = new HashMap<CString, CString>(msgStartup.getParameters());
                newMsgParameters.forEach((k, v) -> {
                    k.retain();
                    if (!k.equals(CString.valueOf("user"))) {
                        v.retain();
                    }
                });
                newMsgParameters.put(CString.valueOf("user"), userIdentificate);
                newMsg.setParameters(newMsgParameters); 
            }
            return newMsg;
        }
        else if (msg instanceof PgsqlAuthenticationCleartextPasswordMessage) {
            // TODO
            return null;
        }
        else if (msg instanceof PgsqlAuthenticationOkMessage) {
            // TODO
            return null;
        }
        else if (msg instanceof PgsqlPasswordMessage) {
            // TODO
            return null;
        }
        return null;
    }

}
