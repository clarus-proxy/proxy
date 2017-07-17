package eu.clarussecure.proxy.protocol.plugins.tcp;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolServer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class TCPServer<CI extends ChannelInitializer<Channel>, SI extends ChannelInitializer<Channel>>
        implements ProtocolServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TCPServer.class);

    private final Configuration configuration;

    private final Class<CI> clientSideChannelInitializerType;

    private final Class<SI> serverSideChannelInitializerType;

    private final int preferredServerEndpoint;

    private volatile boolean ready = false;

    public TCPServer(Configuration configuration, Class<CI> clientSideChannelInitializerType,
            Class<SI> serverSideChannelInitializerType, int preferredServerEndpoint) {
        this.configuration = configuration;
        this.clientSideChannelInitializerType = clientSideChannelInitializerType;
        this.serverSideChannelInitializerType = serverSideChannelInitializerType;
        this.preferredServerEndpoint = preferredServerEndpoint;
    }

    @Override
    public Void call() throws Exception {
        EventLoopGroup acceptorGroup = new NioEventLoopGroup(configuration.getNbListenThreads());
        EventLoopGroup childGroup = new NioEventLoopGroup(configuration.getNbSessionThreads());
        try {
            ChannelInitializer<Channel> clientSidePipelineInitializer = buildClientSidePipelineInitializer();
            ChannelInitializer<Channel> serverSidePipelineInitializer = buildServerSidePipelineInitializer();
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(acceptorGroup, childGroup).channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(configuration.getListenPort()))
                    .childAttr(TCPConstants.CONFIGURATION_KEY, configuration)
                    .childAttr(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY, preferredServerEndpoint)
                    .childAttr(TCPConstants.SERVER_INITIALIZER_KEY, serverSidePipelineInitializer)
                    .childHandler(clientSidePipelineInitializer).childOption(ChannelOption.AUTO_READ, false);
            ChannelFuture f = bootstrap.bind().sync();
            LOGGER.info("Server ready to serve requests at:" + f.channel().localAddress());
            synchronized (this) {
                ready = true;
                notifyAll();
            }
            f.channel().closeFuture().sync();
        } finally {
            acceptorGroup.shutdownGracefully().sync();
            childGroup.shutdownGracefully().sync();
        }
        return null;
    }

    @Override
    public void waitForServerIsReady() throws InterruptedException {
        while (!ready) {
            synchronized (this) {
                if (!ready) {
                    wait();
                }
            }
        }
    }

    protected ChannelInitializer<Channel> buildClientSidePipelineInitializer() {
        try {
            return clientSideChannelInitializerType.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            // Should not occur
            LOGGER.error("Cannot create instance of class {}: ", clientSideChannelInitializerType.getSimpleName(), e);
            throw new IllegalArgumentException(String.format("Cannot create instance of class %s: ",
                    clientSideChannelInitializerType.getSimpleName(), e));
        }
    }

    protected ChannelInitializer<Channel> buildServerSidePipelineInitializer() {
        try {
            return serverSideChannelInitializerType.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            // Should not occur
            LOGGER.error("Cannot create instance of class {}: ", serverSideChannelInitializerType.getSimpleName(), e);
            throw new IllegalArgumentException(String.format("Cannot create instance of class %s: ",
                    serverSideChannelInitializerType.getSimpleName(), e));
        }
    }

}
