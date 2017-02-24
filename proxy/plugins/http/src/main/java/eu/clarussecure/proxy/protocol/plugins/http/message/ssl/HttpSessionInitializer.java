/*
 * 
 */
package eu.clarussecure.proxy.protocol.plugins.http.message.ssl;

import java.io.IOException;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.http.HttpSession;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

// TODO: Auto-generated Javadoc
/**
 * The Class HttpSessionInitializer.
 */
public class HttpSessionInitializer {
	
	/** The Constant LOGGER. */
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpSessionInitializer.class);

	/** The ssl session initializer. */
	private volatile SSLSessionInitializer sslSessionInitializer;
	
	/** The has client side SSL handler. */
	private boolean hasClientSideSSLHandler;
	
	/** The has server side SSL handler. */
	private boolean hasServerSideSSLHandler;
	
	/**
	 * Instantiates a new http session initializer.
	 */
	public HttpSessionInitializer() {
		sslSessionInitializer = new SSLSessionInitializer();
		hasClientSideSSLHandler = false;
		hasServerSideSSLHandler = false;
	}

	/**
	 * Adds the SSL handler on client side.
	 *
	 * @param ctx the ctx
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void addSSLHandlerOnClientSide(ChannelHandlerContext ctx) throws IOException {
		Future<Channel> handshakeFuture = sslSessionInitializer.addSSLHandlerOnClientSide(ctx,
				getHttpSession(ctx).getClientSideChannel().pipeline());
		handshakeFuture.addListener(new GenericFutureListener<Future<? super Channel>>() {

			@Override
			public void operationComplete(Future<? super Channel> future) throws Exception {
				LOGGER.trace("SSL handshake for client side completed");
			}
		});
		hasClientSideSSLHandler = true;
	}

	/**
	 * Adds the SSL handler on server side.
	 *
	 * @param ctx the ctx
	 * @throws SSLException the SSL exception
	 */
	public void addSSLHandlerOnServerSide(ChannelHandlerContext ctx) throws SSLException {
		Future<Channel> handshakeFuture = sslSessionInitializer.addSSLHandlerOnServerSide(ctx);
		handshakeFuture.addListener(new GenericFutureListener<Future<? super Channel>>() {

			@Override
			public void operationComplete(Future<? super Channel> future) throws Exception {
				LOGGER.trace("SSL handshake for server side completed");
			}
		});
		hasServerSideSSLHandler = true;
	}

	/**
	 * Gets the http session.
	 *
	 * @param ctx the ctx
	 * @return the http session
	 */
	private HttpSession getHttpSession(ChannelHandlerContext ctx) {
		HttpSession httpSession = (HttpSession) ctx.channel().attr(TCPConstants.SESSION_KEY).get();
		return httpSession;
	}
		
	/**
	 * Checks for client side SSL handler.
	 *
	 * @return true, if successful
	 */
	public boolean hasClientSideSSLHandler(){
		return hasClientSideSSLHandler;
	}
	
	/**
	 * Checks for server side SSL handler.
	 *
	 * @return true, if successful
	 */
	public boolean hasServerSideSSLHandler(){
		return hasServerSideSSLHandler;
	}
}
