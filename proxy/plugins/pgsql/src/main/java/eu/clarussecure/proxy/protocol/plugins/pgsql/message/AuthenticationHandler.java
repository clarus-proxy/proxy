package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.spi.CString;
import io.netty.channel.ChannelHandlerContext;

public class AuthenticationHandler extends PgsqlMessageHandler<PgsqlAuthenticationMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationHandler.class);
    
    private final static int AUTHENTICATION_OK = 0;
    
    private final static int AUTHENTICATION_CLEARTEXT_PASSWORD = 3;
    
    private final static int AUTHENTICATION_MD5_PASSWORD = 5;
    
    public AuthenticationHandler() {
        super(PgsqlAuthenticationResponse.class, PgsqlStartupMessage.class, PgsqlPasswordMessage.class);
    }

    @Override
    protected PgsqlAuthenticationMessage process(ChannelHandlerContext ctx, PgsqlAuthenticationMessage msg) throws IOException, NoSuchAlgorithmException {
        if (msg instanceof PgsqlStartupMessage) {
            PgsqlStartupMessage msgStartup = (PgsqlStartupMessage) msg;
            LOGGER.debug("User id: {}, database: {}", msgStartup.getParameters().get(CString.valueOf("user")), msgStartup.getParameters().get(CString.valueOf("database")));
            PgsqlStartupMessage newMsg = msgStartup; 
            CString userIdentificate = getEventProcessor(ctx).processUserAuthentication(ctx, msgStartup.getParameters());
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
        }  else if (msg instanceof PgsqlAuthenticationResponse) {
            PgsqlAuthenticationResponse newAuthenticationMessage = (PgsqlAuthenticationResponse) msg; 
            LOGGER.debug("Authentication Type : {}" , newAuthenticationMessage.getAuthenticationType());
            int authenticationType = newAuthenticationMessage.getAuthenticationType();
            switch (authenticationType) {
                case AUTHENTICATION_MD5_PASSWORD:
                // Retrieve authentication parameters (salt, etc.).
                int newAuthenticationType = getEventProcessor(ctx).processAuthenticationParameters(ctx, authenticationType, newAuthenticationMessage.getAuthenticationParameters());
                // If MD5 authentication, force authentication type to Cleartext (3) and remove ByteBuf (salt).
                if (newAuthenticationType != authenticationType) {
                    newAuthenticationMessage = new PgsqlAuthenticationResponse(newAuthenticationType, null);
                }
                case AUTHENTICATION_OK: 
                case AUTHENTICATION_CLEARTEXT_PASSWORD:
                    return newAuthenticationMessage;
                default:
                    throw new UnsupportedOperationException("Unsupported authentication type: " + authenticationType);
            }
        } else if (msg instanceof PgsqlPasswordMessage) {
            PgsqlPasswordMessage msgPassword = (PgsqlPasswordMessage) msg;
            LOGGER.debug("User password : {}", msgPassword.getPassword());
            // Retrieve password.
            CString password = getEventProcessor(ctx).processAuthentication(ctx, msgPassword.getPassword());
            // If MD5, set encrypted password.
            if (password != msgPassword.getPassword()) {
                msgPassword = new PgsqlPasswordMessage(password);
            }
            LOGGER.debug("New password (may be the same) : {}", password);
            return msgPassword;
        }
        return null;
    }

}
