package eu.clarussecure.proxy.protocol.plugins.http.handlers;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

import io.netty.handler.codec.http.HttpResponse;

public class HttpRequestDecoder extends MessageToMessageDecoder<HttpObject> {

	@Override
	protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
		System.out.println(msg.toString());		
	}
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		this.requestProxifier = new RequestProxifier(config, ctx, parserThreadPool);
		super.channelActive(ctx);
	}
}
