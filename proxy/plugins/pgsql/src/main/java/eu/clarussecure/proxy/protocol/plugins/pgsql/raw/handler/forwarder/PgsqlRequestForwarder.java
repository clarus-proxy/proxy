package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.pgsql.BackendSidePipelineInitializer;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawPart;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.ClientMessageForwarder;
import io.netty.channel.ChannelHandlerContext;

public class PgsqlRequestForwarder
        extends ClientMessageForwarder<PgsqlRawPart, BackendSidePipelineInitializer, PgsqlSession> {

    public PgsqlRequestForwarder() {
        super(BackendSidePipelineInitializer.class, PgsqlSession.class);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        PgsqlSession session = (PgsqlSession) ctx.channel().attr(TCPConstants.SESSION_KEY).get();
        session.getSqlSession().reset();
        super.channelInactive(ctx);
    }
}
