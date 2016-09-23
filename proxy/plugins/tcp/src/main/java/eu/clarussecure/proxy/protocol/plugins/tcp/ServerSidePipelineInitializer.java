package eu.clarussecure.proxy.protocol.plugins.tcp;

import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.ServerTCPFrameForwarder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;

public class ServerSidePipelineInitializer extends ChannelInitializer<Channel> {

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("ServerTCPFrameForwarder", new ServerTCPFrameForwarder());
    }

}
