/*
 *
 */
package eu.clarussecure.proxy.protocol.plugins.wfs;

import eu.clarussecure.proxy.protocol.plugins.http.handler.codec.HttpHeaderCodec;
import eu.clarussecure.proxy.protocol.plugins.http.handler.decoder.HttpObjectAccumulator;
import eu.clarussecure.proxy.protocol.plugins.http.handler.forwarder.HttpResponseForwarder;
import eu.clarussecure.proxy.protocol.plugins.http.message.SessionInitializationResponseHandler;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.WfsResponseDecoder;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * Created on 09/06/2017.
 */
public class WfsServerPipelineInitializer extends ChannelInitializer<Channel> {

    public WfsServerPipelineInitializer() {
        super();
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        Configuration configuration = ch.attr(TCPConstants.CONFIGURATION_KEY).get();
        ChannelPipeline pipeline = ch.pipeline();

        EventExecutorGroup parserGroup = new DefaultEventExecutorGroup(configuration.getNbParserThreads());
        pipeline.addLast(parserGroup, "SessionInitializationResponseHandler",
                new SessionInitializationResponseHandler());
        pipeline.addLast(parserGroup, "HttpClientCodec", new HttpClientCodec());
        pipeline.addLast(parserGroup, "HttpContentDecompressor", new HttpContentDecompressor());
        pipeline.addLast(parserGroup, "HttpHeaderCodec", new HttpHeaderCodec());

        //pipeline.addLast(parserGroup, "HttpObjectAggregator", new HttpObjectAggregator(512 * 1024));
        //pipeline.addLast(parserGroup, "HttpObjectAccumulator", new HttpObjectAccumulator(Integer.MAX_VALUE));
        pipeline.addLast(parserGroup, "WfsResponseDecoder", new WfsResponseDecoder());

        pipeline.addLast(parserGroup, "HttpResponseForwarder", new HttpResponseForwarder());
    }

}
