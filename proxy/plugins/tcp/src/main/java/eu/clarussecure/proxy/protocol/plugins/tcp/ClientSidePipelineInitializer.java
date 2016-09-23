package eu.clarussecure.proxy.protocol.plugins.tcp;

import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.ClientTCPFrameForwarder;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class ClientSidePipelineInitializer extends ChannelInitializer<Channel> {

    @Override
    protected void initChannel(Channel ch) throws Exception {
        Configuration configuration = ch.attr(TCPConstants.CONFIGURATION_KEY).get();
        ChannelPipeline pipeline = ch.pipeline();
        EventExecutorGroup parserGroup = null;
        if (configuration.getNbParserThreads() > 0) {
            parserGroup = new DefaultEventExecutorGroup(configuration.getNbParserThreads());
        }
        pipeline.addLast(parserGroup, "ClientTCPFrameForwarder", new ClientTCPFrameForwarder());
    }

}
