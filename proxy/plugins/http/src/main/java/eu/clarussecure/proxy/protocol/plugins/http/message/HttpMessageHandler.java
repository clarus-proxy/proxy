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
 * The Class HttpMessageHandler.
 */
public abstract class HttpMessageHandler extends MessageToMessageDecoder<HttpObject> {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpMessageHandler.class);

    /*
     * (non-Javadoc)
     * 
     * @see
     * io.netty.handler.codec.MessageToMessageDecoder#decode(io.netty.channel.
     * ChannelHandlerContext, java.lang.Object, java.util.List)
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
        LOGGER.debug("Decoding request message...");
        ReferenceCountUtil.retain(msg);
        out.add(msg);
    }

    /**
     * Gets the http session.
     *
     * @param ctx
     *            the ctx
     * @return the http session
     */
    protected HttpSession getHttpSession(ChannelHandlerContext ctx) {
        HttpSession httpSession = (HttpSession) ctx.channel().attr(TCPConstants.SESSION_KEY).get();
        return httpSession;
    }

    /**
     * Gets the configuration.
     *
     * @param ctx
     *            the ctx
     * @return the configuration
     */
    protected HttpConfiguration getConfiguration(ChannelHandlerContext ctx) {
        HttpConfiguration configuration = (HttpConfiguration) ctx.channel().attr(TCPConstants.CONFIGURATION_KEY).get();
        return configuration;
    }
}
