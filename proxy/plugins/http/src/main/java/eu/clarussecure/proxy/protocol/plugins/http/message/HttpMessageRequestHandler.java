/*
 * 
 */
package eu.clarussecure.proxy.protocol.plugins.http.message;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.http.HttpConfiguration;
import eu.clarussecure.proxy.protocol.plugins.http.HttpSession;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.HttpObject;
import io.netty.util.ReferenceCountUtil;

// TODO: Auto-generated Javadoc
/**
 * The Class HttpMessageRequestHandler.
 */
public abstract class HttpMessageRequestHandler extends HttpMessageHandler {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpMessageRequestHandler.class);

	/* (non-Javadoc)
	 * @see io.netty.handler.codec.MessageToMessageDecoder#decode(io.netty.channel.ChannelHandlerContext, java.lang.Object, java.util.List)
	 */
	@Override
	protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
		LOGGER.debug("Decoding request message...");
		ReferenceCountUtil.retain(msg);
		out.add(msg);
	}

	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#channelActive(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		HttpConfiguration configuration = getConfiguration(ctx);
		HttpSession httpSession = getHttpSession(ctx);
		if (httpSession != null && configuration.isClientSSLMode() && httpSession.getSessionInitializer().hasClientSideSSLHandler() == false) {
			getHttpSession(ctx).getSessionInitializer().addSSLHandlerOnClientSide(ctx);
			LOGGER.debug("Handler has been set on client side.");
		}
		super.channelActive(ctx);
		LOGGER.debug("Channel '" + ctx.name() + "' is active.");
	}
	
}
