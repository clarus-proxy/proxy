package eu.clarussecure.proxy.protocol.plugins.http.handlers;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

import io.netty.handler.codec.http.HttpResponse;

public class HttpHeaderCodec extends MessageToMessageCodec<HttpRequest, HttpResponse>{

	@Override
	protected void encode(ChannelHandlerContext ctx, HttpResponse msg, List<Object> out) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, HttpRequest msg, List<Object> out) throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	
}
