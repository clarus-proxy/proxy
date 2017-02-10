package eu.clarussecure.proxy.protocol.plugins.http.raw.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.http.HttpServerPipelineInitializer;
import eu.clarussecure.proxy.protocol.plugins.http.HttpSession;
import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.ClientMessageForwarder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpObject;

public class HttpRequestForwarder
		extends ClientMessageForwarder<HttpObject, HttpServerPipelineInitializer, HttpSession> {

	public HttpRequestForwarder() {
		super(HttpServerPipelineInitializer.class, HttpSession.class);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
	}
}
