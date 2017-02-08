package eu.clarussecure.proxy.protocol.plugins.http.raw.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.http.BackendSidePipelineInitializer;
import eu.clarussecure.proxy.protocol.plugins.http.HttpSession;
import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.ClientMessageForwarder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpObject;

public class HttpRequestForwarder
		extends ClientMessageForwarder<HttpObject, BackendSidePipelineInitializer, HttpSession> {

	public HttpRequestForwarder() {
		super(BackendSidePipelineInitializer.class, HttpSession.class);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
	}
}
