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
 * The Class HttpMessageResponseHandler.
 */
public abstract class HttpMessageResponseHandler extends HttpMessageHandler {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpMessageResponseHandler.class);

    /*
     * (non-Javadoc)
     * 
     * @see
     * io.netty.channel.ChannelInboundHandlerAdapter#channelActive(io.netty.
     * channel.ChannelHandlerContext)
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        HttpConfiguration configuration = getConfiguration(ctx);
        HttpSession httpSession = getHttpSession(ctx);
        if (httpSession != null && configuration.isServerSSLMode()
                && httpSession.getSessionInitializer().hasServerSideSSLHandler() == false) {
            getHttpSession(ctx).getSessionInitializer().addSSLHandlerOnServerSide(ctx);
            LOGGER.debug("Handler has been set on server side.");
        }
        super.channelActive(ctx);
        LOGGER.debug("Channel '" + ctx.name() + "' is active.");
    }

}
