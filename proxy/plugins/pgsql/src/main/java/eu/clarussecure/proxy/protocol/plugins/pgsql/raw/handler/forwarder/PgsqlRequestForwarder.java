package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlClient;
import io.netty.channel.ChannelHandlerContext;

public class PgsqlRequestForwarder extends PgsqlRawPartForwarder {

    public PgsqlRequestForwarder() {
        super(true);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug("{} new client connection from {}", direction, ctx.channel().remoteAddress());
        sinkChannel = new PgsqlClient(ctx).call();
        super.channelActive(ctx);
    }

}
