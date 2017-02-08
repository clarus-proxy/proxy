package eu.clarussecure.proxy.protocol.plugins.http;

import eu.clarussecure.proxy.protocol.plugins.http.raw.handler.codec.HttpHeaderCodec;
import eu.clarussecure.proxy.protocol.plugins.http.raw.handler.forwarder.HttpResponseForwarder;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class BackendSidePipelineInitializer extends ChannelInitializer<Channel> {

	public BackendSidePipelineInitializer() {
		super();
	}

	@Override
	protected void initChannel(Channel ch) throws Exception {
		Configuration configuration = ch.attr(TCPConstants.CONFIGURATION_KEY).get();
		ChannelPipeline pipeline = ch.pipeline();

		EventExecutorGroup parserGroup = new DefaultEventExecutorGroup(configuration.getNbParserThreads());
		// Session initialization consists of dealing with optional
		// initialization of SSL encryption: a specific SSL handler will be
		// added as first handler in the pipeline if necessary
		// The session initialization handler will be removed while dealing with
		// the startup message (by the SessionInitializationRequestHandler
		// running on the frontend side).
		pipeline.addLast(parserGroup, "HttpClientCodec", new HttpClientCodec());
		pipeline.addLast(parserGroup, "HttpContentDecompressor", new HttpContentDecompressor());
		pipeline.addLast(parserGroup, "HttpHeaderCodec", new HttpHeaderCodec());
		pipeline.addLast(parserGroup, "HttpResponseForwarder", new HttpResponseForwarder());
	}

}
