package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class PgsqlServer implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgsqlServer.class);

    private final PgsqlConfiguration configuration;

    public PgsqlServer(PgsqlConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Void call() throws Exception {
        EventLoopGroup acceptorGroup = new NioEventLoopGroup(configuration.getNbAcceptorThreads());
        EventLoopGroup childGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(acceptorGroup, childGroup)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(configuration.getListenPort()))
                    .childAttr(PgsqlConstants.CONFIGURATION_KEY, configuration)
                    .childHandler(new FrontendPipelineInitializer())
                    .childOption(ChannelOption.AUTO_READ, false);
            ChannelFuture f = bootstrap.bind().sync();
            LOGGER.info("Server ready to serve requests at:" + f.channel().localAddress());
            f.channel().closeFuture().sync();
        } finally {
            acceptorGroup.shutdownGracefully().sync();
            childGroup.shutdownGracefully().sync();
        }
        return null;
    }

}
