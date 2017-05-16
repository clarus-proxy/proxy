/*
 * 
 */
package eu.clarussecure.proxy.protocol.plugins.http.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.http.HttpConfiguration;
import eu.clarussecure.proxy.protocol.plugins.http.HttpSession;
import io.netty.channel.ChannelHandlerContext;

// TODO: Auto-generated Javadoc
/**
 * The Class HttpMessageRequestHandler.
 */
public abstract class HttpMessageRequestHandler extends HttpMessageHandler {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpMessageRequestHandler.class);
	
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
