package eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
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
        ctx.channel().config().setAutoRead(true);
        TCPSession session = ctx.channel().attr(TCPConstants.SESSION_KEY).get();
        boolean allConnectionCompleted = session.decrementAndGetExpectedConnections() == 0;
        if (allConnectionCompleted) {
            Channel clientSideChannel = session.getClientSideChannel();
            clientSideChannel.config().setAutoRead(true);
        }
        super.channelActive(ctx);
        if (allConnectionCompleted) {
            if (LOGGER.isInfoEnabled()) {
                StringBuilder sb = new StringBuilder();
                for (Channel serverSidechannel : session.getServerSideChannels()) {
                    if (sb.length() > 0) {
                        sb.append(", ");
                    }
                    sb.append(serverSidechannel.remoteAddress());
                }
                LOGGER.info("Intercepting traffic between {} and {}", session.getClientSideChannel().remoteAddress(),
                        sb.toString());
            }
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, I msg) throws Exception {
        boolean forward;
        if (msg instanceof FilterableMessage && ((FilterableMessage) msg).filter()) {
            Integer preferredServerEndpoint = ctx.channel().attr(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY).get();
            if (preferredServerEndpoint == null) {
                throw new NullPointerException(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY.name() + " is not set");
            }
            Integer serverEndpointNumber = ctx.channel().attr(TCPConstants.SERVER_ENDPOINT_NUMBER_KEY).get();
            if (serverEndpointNumber == null) {
                throw new NullPointerException(TCPConstants.SERVER_ENDPOINT_NUMBER_KEY.name() + " is not set");
            }
            forward = preferredServerEndpoint == -1 || preferredServerEndpoint == serverEndpointNumber;
        } else {
            forward = true;
        }
        if (forward) {
            Channel sinkChannel = getSinkChannels(ctx).get(0);
            forwardMessage(msg, sinkChannel);
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Message not forwarded");
            }
        }
    }
}
