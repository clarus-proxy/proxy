/*
 * 
 */
package eu.clarussecure.proxy.protocol.plugins.http;

import eu.clarussecure.proxy.protocol.plugins.http.message.SessionInitializationResponseHandler;
import eu.clarussecure.proxy.protocol.plugins.http.handler.codec.HttpHeaderCodec;
import eu.clarussecure.proxy.protocol.plugins.http.handler.forwarder.HttpResponseForwarder;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

// TODO: Auto-generated Javadoc
/**
 * The Class HttpServerPipelineInitializer.
 */
public class HttpServerPipelineInitializer extends ChannelInitializer<Channel> {

	/**
	 * Instantiates a new http server pipeline initializer.
	 */
	public HttpServerPipelineInitializer() {
		super();
	}

	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelInitializer#initChannel(io.netty.channel.Channel)
	 */
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
		pipeline.addLast(parserGroup, "HttpResponseForwarder", new HttpResponseForwarder());
	}

}
