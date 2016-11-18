package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawPart;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.ServerMessageForwarder;
import io.netty.channel.ChannelHandlerContext;

public class PgsqlResponseForwarder extends ServerMessageForwarder<PgsqlRawPart> {

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        PgsqlSession session = (PgsqlSession) ctx.channel().attr(TCPConstants.SESSION_KEY).get();
        session.getSqlSession().reset();
        super.channelInactive(ctx);
    }
}
