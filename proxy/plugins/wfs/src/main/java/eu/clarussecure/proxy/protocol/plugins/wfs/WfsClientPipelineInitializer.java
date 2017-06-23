package eu.clarussecure.proxy.protocol.plugins.wfs;

import eu.clarussecure.proxy.protocol.plugins.http.handler.codec.HttpHeaderCodec;
import eu.clarussecure.proxy.protocol.plugins.http.handler.forwarder.HttpRequestForwarder;
import eu.clarussecure.proxy.protocol.plugins.http.message.SessionInitializationRequestHandler;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.WfsRequestDecoder;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class WfsClientPipelineInitializer extends ChannelInitializer<Channel> {

    /*
     * (non-Javadoc)
     *
     * @see
     * io.netty.channel.ChannelInitializer#initChannel(io.netty.channel.Channel)
     */
    @Override
    protected void initChannel(Channel ch) throws Exception {
        Configuration configuration = ch.attr(TCPConstants.CONFIGURATION_KEY).get();
        ChannelPipeline pipeline = ch.pipeline();

        EventExecutorGroup parserGroup = new DefaultEventExecutorGroup(configuration.getNbParserThreads());
        pipeline.addLast(parserGroup, "SessionInitializationRequestHandler", new SessionInitializationRequestHandler());
        pipeline.addLast(parserGroup, "HttpServerCodec", new HttpServerCodec());
        pipeline.addLast(parserGroup, "HttpHeaderCodec", new HttpHeaderCodec());

        pipeline.addLast(parserGroup, "WfsRequestDecoder", new WfsRequestDecoder());

        pipeline.addLast(parserGroup, "HttpRequestForwarder", new HttpRequestForwarder());
    }

}
