package eu.clarussecure.proxy.protocol.plugins.http.raw.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.ServerMessageForwarder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpObject;

public class HttpResponseForwarder extends ServerMessageForwarder<HttpObject> {

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
	}
}
