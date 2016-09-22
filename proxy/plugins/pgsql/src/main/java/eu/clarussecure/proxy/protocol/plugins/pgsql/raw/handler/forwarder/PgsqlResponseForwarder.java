package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConfiguration;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConstants;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import io.netty.channel.ChannelHandlerContext;

public class PgsqlResponseForwarder extends PgsqlRawPartForwarder {

    public PgsqlResponseForwarder() {
        super(false);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            PgsqlConfiguration configuration = ctx.channel().attr(PgsqlConstants.CONFIGURATION_KEY).get();
            LOGGER.debug("{} connection to {} completed", direction, configuration.getServerEndpoint());
        }
        PgsqlSession session = ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        sinkChannel = session.getClientSideChannel();
        ctx.channel().config().setAutoRead(true);
        sinkChannel.config().setAutoRead(true);
        super.channelActive(ctx);
        LOGGER.info("Intercepting traffic between {} and {}", session.getClientSideChannel().remoteAddress(), ctx.channel().remoteAddress());
    }

}
