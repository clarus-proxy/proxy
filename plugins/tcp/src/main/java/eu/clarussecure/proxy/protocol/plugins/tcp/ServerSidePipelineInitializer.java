package eu.clarussecure.proxy.protocol.plugins.tcp;

import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.ServerTCPFrameForwarder;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class ServerSidePipelineInitializer extends ChannelInitializer<Channel> {

    private EventExecutorGroup parserGroup = null;

    @Override
    protected void initChannel(Channel ch) throws Exception {
        Configuration configuration = ch.attr(TCPConstants.CONFIGURATION_KEY).get();
        ChannelPipeline pipeline = ch.pipeline();
        if (parserGroup == null && configuration.getNbParserThreads() > 0) {
            parserGroup = new DefaultEventExecutorGroup(configuration.getNbParserThreads());
        }
        pipeline.addLast(parserGroup, "ServerTCPFrameForwarder", new ServerTCPFrameForwarder());
    }

}
