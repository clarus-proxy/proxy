package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.spi.CString;
import io.netty.channel.ChannelHandlerContext;

public class AuthenticationHandler extends PgsqlMessageHandler<PgsqlStartupMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationHandler.class);

    public AuthenticationHandler() {
        super(PgsqlStartupMessage.class);
    }

    @Override
    protected PgsqlStartupMessage process(ChannelHandlerContext ctx, PgsqlStartupMessage msg) throws IOException {
        LOGGER.debug("User id: {}, database: {}", msg.getParameters().get(CString.valueOf("user")), msg.getParameters().get(CString.valueOf("database")));
        PgsqlStartupMessage newMsg = new PgsqlStartupMessage(msg.getProtocolVersion(), msg.getParameters());
        CString userIdentificate = getEventProcessor(ctx).processAuthentication(ctx, msg.getParameters());
        CString userMsg = msg.getParameters().get(CString.valueOf("user"));
        if (!userIdentificate.equals(userMsg)) {
            Map<CString, CString> newMsgParameters = new HashMap<CString, CString>(msg.getParameters());
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

}
