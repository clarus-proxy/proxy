package eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.ChannelHandlerContext;

public class ServerMessageForwarder<I> extends MessageForwarder<I> {

    public ServerMessageForwarder() {
        super(false);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            Configuration configuration = ctx.channel().attr(TCPConstants.CONFIGURATION_KEY).get();
            LOGGER.debug("{} connection to {} completed", direction, configuration.getServerEndpoint());
        }
        TCPSession session = ctx.channel().attr(TCPConstants.SESSION_KEY).get();
        sinkChannel = session.getClientSideChannel();
        ctx.channel().config().setAutoRead(true);
        sinkChannel.config().setAutoRead(true);
        super.channelActive(ctx);
        LOGGER.info("Intercepting traffic between {} and {}", session.getClientSideChannel().remoteAddress(), ctx.channel().remoteAddress());
    }

}
