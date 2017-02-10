package eu.clarussecure.proxy.protocol.plugins.http;

import eu.clarussecure.proxy.protocol.plugins.http.message.SessionInitializationRequestHandler;
import eu.clarussecure.proxy.protocol.plugins.http.raw.handler.codec.HttpHeaderCodec;
import eu.clarussecure.proxy.protocol.plugins.http.raw.handler.forwarder.HttpRequestForwarder;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class HttpClientPipelineInitializer extends ChannelInitializer<Channel> {

	@Override
	protected void initChannel(Channel ch) throws Exception {
		Configuration configuration = ch.attr(TCPConstants.CONFIGURATION_KEY).get();
		ChannelPipeline pipeline = ch.pipeline();
		EventExecutorGroup parserGroup = new DefaultEventExecutorGroup(configuration.getNbParserThreads());
		// Session initialization consists of dealing with optional
		// initialization of SSL encryption: a specific SSL handler will be
		// added as first handler in the pipeline if necessary
		// Session initialization ends with the startup message. Then the
		// session initialization handler will be removed
		pipeline.addLast(parserGroup, "SessionInitializationRequestHandler", new SessionInitializationRequestHandler());
		pipeline.addLast(parserGroup, "HttpServerCodec", new HttpServerCodec());
		pipeline.addLast(parserGroup, "HttpHeaderCodec", new HttpHeaderCodec());
		pipeline.addLast(parserGroup, "HttpRequestForwarder", new HttpRequestForwarder());
	}

}
