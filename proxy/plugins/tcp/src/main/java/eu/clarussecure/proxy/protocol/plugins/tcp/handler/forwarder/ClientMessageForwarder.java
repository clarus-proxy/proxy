package eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPClient;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;

public abstract class ClientMessageForwarder<I, CI extends ChannelInitializer<Channel>, S extends TCPSession> extends MessageForwarder<I> {

    private Class<CI> channelInitializerType;

    private Class<S> sessionType;

    public ClientMessageForwarder(Class<CI> channelInitializerType, Class<S> sessionType) {
        super(true);
        this.channelInitializerType = channelInitializerType;
        this.sessionType = sessionType;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug("{} new client connection from {}", direction, ctx.channel().remoteAddress());
        TCPClient<? extends ChannelInitializer<Channel>, ? extends TCPSession> client = new TCPClient<>(ctx, channelInitializerType, sessionType);
        sinkChannel = client.call();
        super.channelActive(ctx);
    }
}
