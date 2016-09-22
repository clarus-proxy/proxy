package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;

public class PgsqlClient implements Callable<Channel> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(PgsqlClient.class);

    private final ChannelHandlerContext ctx;

    public PgsqlClient(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public Channel call() throws Exception {
        Channel clientSideChannel = ctx.channel();
        
        PgsqlConfiguration configuration = clientSideChannel.attr(PgsqlConstants.CONFIGURATION_KEY).get();
        Bootstrap bootstrap = new Bootstrap();
        PgsqlSession session = new PgsqlSession();
        session.setClientSideChannel(clientSideChannel);
        clientSideChannel.attr(PgsqlConstants.SESSION_KEY).set(session);
        bootstrap.group(clientSideChannel.eventLoop())
                .channel(NioSocketChannel.class)
                .attr(PgsqlConstants.CONFIGURATION_KEY, configuration)
                .attr(PgsqlConstants.SESSION_KEY, session)
                .handler(new BackendPipelineInitializer(ctx.executor()))
                .option(ChannelOption.AUTO_READ, false);
        LOGGER.trace("Initialize connection to {}", configuration.getServerEndpoint());
        ChannelFuture connectFuture = bootstrap.connect(configuration.getServerEndpoint());
        Channel serverSideChannel = connectFuture.channel();
        return serverSideChannel;
    }

}
