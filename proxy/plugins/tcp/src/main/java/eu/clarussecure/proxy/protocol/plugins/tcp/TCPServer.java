package eu.clarussecure.proxy.protocol.plugins.tcp;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class TCPServer<CI extends ChannelInitializer<Channel>> implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TCPServer.class);

    private final Configuration configuration;

    private final Class<CI> channelInitializerType;

    public TCPServer(Configuration configuration, Class<CI> channelInitializerType) {
        this.configuration = configuration;
        this.channelInitializerType = channelInitializerType;
    }

    @Override
    public Void call() throws Exception {
        EventLoopGroup acceptorGroup = new NioEventLoopGroup(configuration.getNbListenThreads());
        EventLoopGroup childGroup = new NioEventLoopGroup(configuration.getNbSessionThreads());
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(acceptorGroup, childGroup).channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(configuration.getListenPort()))
                    .childAttr(TCPConstants.CONFIGURATION_KEY, configuration)
                    .childHandler(buildClientSidePipelineInitializer()).childOption(ChannelOption.AUTO_READ, false);
            ChannelFuture f = bootstrap.bind().sync();
            LOGGER.info("Server ready to serve requests at:" + f.channel().localAddress());
            f.channel().closeFuture().sync();
        } finally {
            acceptorGroup.shutdownGracefully().sync();
            childGroup.shutdownGracefully().sync();
        }
        return null;
    }

    protected ChannelInitializer<Channel> buildClientSidePipelineInitializer() {
        try {
            return channelInitializerType.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            // Should not occur
            LOGGER.error("Cannot create instance of class {}: ", channelInitializerType.getSimpleName(), e);
            throw new IllegalArgumentException(
                    String.format("Cannot create instance of class %s: ", channelInitializerType.getSimpleName(), e));
        }
    }
}
