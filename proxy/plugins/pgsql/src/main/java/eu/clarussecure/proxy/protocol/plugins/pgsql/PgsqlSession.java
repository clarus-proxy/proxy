package eu.clarussecure.proxy.protocol.plugins.pgsql;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.AuthenticationProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.CommandProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SqlSession;
import io.netty.channel.Channel;

public class PgsqlSession {

    private Channel clientSideChannel;

    private AuthenticationProcessor authenticationProcessor;

    private CommandProcessor commandProcessor;

    private SqlSession session;
    
    public Channel getClientSideChannel() {
        return clientSideChannel;
    }

    public void setClientSideChannel(Channel clientSideChannel) {
        this.clientSideChannel = clientSideChannel;
    }

    public SqlSession getSession() {
        if (session == null) {
            session = new SqlSession();
        }
        return session;
    }

    public AuthenticationProcessor getAuthenticationProcessor() {
        if (authenticationProcessor == null) {
            authenticationProcessor = new AuthenticationProcessor();
        }
        return authenticationProcessor;
    }

    public void setAuthenticationProcessor(AuthenticationProcessor authenticationProcessor) {
        this.authenticationProcessor = authenticationProcessor;
    }

    public CommandProcessor getCommandProcessor() {
        if (commandProcessor == null) {
            commandProcessor = new CommandProcessor();
        }
        return commandProcessor;
    }

    public void setCommandProcessor(CommandProcessor commandProcessor) {
        this.commandProcessor = commandProcessor;
    }
}
