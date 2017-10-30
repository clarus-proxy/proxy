package eu.clarussecure.proxy.protocol.plugins.wfs;

import eu.clarussecure.proxy.protocol.plugins.http.handler.codec.HttpHeaderCodec;
import eu.clarussecure.proxy.protocol.plugins.http.handler.decoder.HttpObjectAccumulator;
import eu.clarussecure.proxy.protocol.plugins.http.handler.forwarder.HttpRequestForwarder;
import eu.clarussecure.proxy.protocol.plugins.http.message.SessionInitializationRequestHandler;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.WfsRequestDecoder;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * Created on 15/06/2017.
 */
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
        pipeline.addLast("SessionInitializationRequestHandler", new SessionInitializationRequestHandler());
        pipeline.addLast("HttpServerCodec", new HttpServerCodec());
        pipeline.addLast("HttpHeaderCodec", new HttpHeaderCodec());

        pipeline.addLast(parserGroup, "WfsRequestAggregator", new HttpObjectAggregator(512 * 1024));
        //pipeline.addLast(parserGroup, "HttpObjectAccumulator", new HttpObjectAccumulator(Integer.MAX_VALUE));
        pipeline.addLast("WfsRequestDecoder", new WfsRequestDecoder(parserGroup));

        pipeline.addLast("HttpRequestForwarder", new HttpRequestForwarder());
    }

}
