package eu.clarussecure.proxy.protocol.plugins.tcp;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;

public class TCPClient<CI extends ChannelInitializer<Channel>, S extends TCPSession> implements Callable<Channel> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(TCPClient.class);

    private final ChannelHandlerContext ctx;
    
    private final Class<CI> channelInitializerType;
    
    private final Class<S> sessionType;

    public TCPClient(ChannelHandlerContext ctx, Class<CI> channelInitializerType, Class<S> sessionType) {
        this.ctx = ctx;
        this.channelInitializerType = channelInitializerType;
        this.sessionType = sessionType;
    }

    @Override
    public Channel call() throws Exception {
        Channel clientSideChannel = ctx.channel();

        Configuration configuration = clientSideChannel.attr(TCPConstants.CONFIGURATION_KEY).get();
        Bootstrap bootstrap = new Bootstrap();
        TCPSession session = buildSession();
        session.setClientSideChannel(clientSideChannel);
        clientSideChannel.attr(TCPConstants.SESSION_KEY).set(session);
        bootstrap.group(clientSideChannel.eventLoop())
                .channel(NioSocketChannel.class)
                .attr(TCPConstants.CONFIGURATION_KEY, configuration)
                .attr(TCPConstants.SESSION_KEY, session)
                .handler(buildServerSidePipelineInitializer())
                .option(ChannelOption.AUTO_READ, false);
        LOGGER.trace("Initialize connection to {}", configuration.getServerEndpoint());
        ChannelFuture connectFuture = bootstrap.connect(configuration.getServerEndpoint());
        Channel serverSideChannel = connectFuture.channel();
        session.setServerSideChannel(serverSideChannel);
        return serverSideChannel;
    }

    protected ChannelInitializer<Channel> buildServerSidePipelineInitializer() {
        try {
            return channelInitializerType.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            // Should not occur
            LOGGER.error("Cannot create instance of class {}: ", channelInitializerType.getSimpleName(), e);
            throw new IllegalArgumentException(String.format("Cannot create instance of class %s: ", channelInitializerType.getSimpleName(), e));
        }
    }

    protected TCPSession buildSession() {
        try {
            return sessionType.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            // Should not occur
            LOGGER.error("Cannot create instance of class {}: ", sessionType.getSimpleName(), e);
            throw new IllegalArgumentException(String.format("Cannot create instance of class %s: ", sessionType.getSimpleName(), e));
        }
    }

}
