package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession.AuthenticationPhase;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawPart;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.ServerMessageForwarder;
import io.netty.channel.ChannelHandlerContext;

public class PgsqlResponseForwarder extends ServerMessageForwarder<PgsqlRawPart> {

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (closeSinkChannels(ctx)) {
            PgsqlSession pgsqlSession = (PgsqlSession) ctx.channel().attr(TCPConstants.SESSION_KEY).get();
            SQLSession session = pgsqlSession.getSqlSession();
            session.reset();
        }
        super.channelInactive(ctx);
    }

    @Override
    protected boolean closeSinkChannels(ChannelHandlerContext ctx) {
        PgsqlSession pgsqlSession = (PgsqlSession) ctx.channel().attr(TCPConstants.SESSION_KEY).get();
        SQLSession session = pgsqlSession.getSqlSession();
        AuthenticationPhase authenticationPhase = session.getAuthenticationPhase();
        if (authenticationPhase != null) {
            boolean allResponsesReceived = authenticationPhase.areAllAuthenticationResponsesReceived();
            if (!allResponsesReceived) {
                return false;
            }
        }
        return true;
    }
}
