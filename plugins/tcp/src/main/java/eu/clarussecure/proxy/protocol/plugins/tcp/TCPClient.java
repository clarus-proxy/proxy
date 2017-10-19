package eu.clarussecure.proxy.protocol.plugins.tcp;

import java.net.InetSocketAddress;
import java.util.Map;
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

public class TCPClient<S extends TCPSession> implements Callable<Void> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(TCPClient.class);

    private final ChannelHandlerContext ctx;

    private final Class<S> sessionType;

    public TCPClient(ChannelHandlerContext ctx, Class<S> sessionType) {
        this.ctx = ctx;
        this.sessionType = sessionType;
    }

    @Override
    public Void call() throws Exception {
        Channel clientSideChannel = ctx.channel();

        Configuration configuration = clientSideChannel.attr(TCPConstants.CONFIGURATION_KEY).get();
        Map<String, Object> customData = clientSideChannel.attr(TCPConstants.CUSTOM_DATA_KEY).get();
        ChannelInitializer<Channel> serverSidePipelineInitializer = clientSideChannel
                .attr(TCPConstants.SERVER_INITIALIZER_KEY).get();
        Bootstrap bootstrap = new Bootstrap();
        TCPSession session = buildSession();
        session.setClientSideChannel(clientSideChannel);
        clientSideChannel.attr(TCPConstants.SESSION_KEY).set(session);
        bootstrap.group(clientSideChannel.eventLoop()).channel(NioSocketChannel.class)
                .attr(TCPConstants.CONFIGURATION_KEY, configuration).attr(TCPConstants.CUSTOM_DATA_KEY, customData)
                .attr(TCPConstants.SESSION_KEY, session)
                .attr(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY,
                        clientSideChannel.attr(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY).get())
                .handler(serverSidePipelineInitializer).option(ChannelOption.AUTO_READ, false);
        session.setExpectedConnections(configuration.getServerEndpoints().size());
        for (int i = 0; i < configuration.getServerEndpoints().size(); i++) {
            InetSocketAddress serverEndpoint = configuration.getServerEndpoints().get(i);
            LOGGER.trace("Initialize connection to {}", serverEndpoint);
            ChannelFuture connectFuture = bootstrap.connect(serverEndpoint);
            Channel serverSideChannel = connectFuture.channel();
            session.addServerSideChannel(serverSideChannel);
            serverSideChannel.attr(TCPConstants.SERVER_ENDPOINT_NUMBER_KEY).set(i);
        }
        return null;
    }

    protected TCPSession buildSession() {
        try {
            return sessionType.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            // Should not occur
            LOGGER.error("Cannot create instance of class {}: ", sessionType.getSimpleName(), e);
            throw new IllegalArgumentException(
                    String.format("Cannot create instance of class %s: ", sessionType.getSimpleName(), e));
        }
    }

}
