package eu.clarussecure.proxy.protocol.plugins.http;

import eu.clarussecure.proxy.protocol.plugins.http.handlers.HttpResponseDecoder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.ssl.SslContext;

public class BackendSidePipelineInitializer extends ChannelInitializer<Channel> {

	private final SslContext sslCtx;

	public BackendSidePipelineInitializer(SslContext sslCtx) {
		this.sslCtx = sslCtx;
	}

	public BackendSidePipelineInitializer() {
		this.sslCtx = null;
	}
	
	@Override
	public void initChannel(Channel ch) {
		ChannelPipeline p = ch.pipeline();
		if (sslCtx != null) {
			p.addLast(sslCtx.newHandler(ch.alloc()));
		}

		p.addLast(new HttpRequestDecoder());
		// Uncomment the following line if you don't want to handle HttpChunks.
		// p.addLast(new HttpObjectAggregator(1048576));
		p.addLast(new HttpResponseEncoder());
		// Remove the following line if you don't want automatic content
		// compression.
		// p.addLast(new HttpContentCompressor());
		p.addLast(new HttpResponseDecoder());

	}
}
