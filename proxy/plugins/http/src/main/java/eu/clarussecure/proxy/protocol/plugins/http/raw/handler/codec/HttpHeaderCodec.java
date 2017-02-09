package eu.clarussecure.proxy.protocol.plugins.http.raw.handler.codec;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.http.HttpConfiguration;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.ReferenceCountUtil;

public class HttpHeaderCodec extends MessageToMessageCodec<HttpRequest, HttpResponse> {

	private static final String SDCH_ENCODING = "sdch";

	private static final Logger LOGGER = LoggerFactory.getLogger(HttpHeaderCodec.class);

	private String requestHost;

	public HttpHeaderCodec() {
		super();
	}

	@Override
	protected void encode(ChannelHandlerContext ctx, HttpResponse response, List<Object> out) throws Exception {
		HttpHeaders headers = response.headers();
		rewriteLocationHeader(ctx, headers);
		ReferenceCountUtil.retain(response);
		out.add(response);
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, HttpRequest request, List<Object> out) throws Exception {
		HttpHeaders headers = request.headers();
		rewriteHostHeader(ctx, headers);
		removeSDCHEncoding(headers);
		ReferenceCountUtil.retain(request);
		out.add(request);
	}

	/**
	 * Rewrite Host header according to targeted backend
	 * 
	 * @param headers
	 */
	private void rewriteHostHeader(ChannelHandlerContext ctx, HttpHeaders headers) {
		this.requestHost = headers.get(HttpHeaderNames.HOST);
		if (this.requestHost != null && !this.requestHost.isEmpty()) {
			HttpConfiguration config = (HttpConfiguration) ctx.channel().attr(TCPConstants.CONFIGURATION_KEY).get();
			headers.remove(HttpHeaderNames.HOST);
			headers.add(HttpHeaderNames.HOST, config.getServerEndpoint().getAddress().getHostAddress() + ":"
					+ config.getServerEndpoint().getPort());
		}
	}

	/**
	 * Remove sdch from encodings we accept since we can't decode it.
	 * 
	 * @param headers
	 *            The headers to modify
	 */
	private void removeSDCHEncoding(HttpHeaders headers) {
		String ae = headers.get(HttpHeaderNames.ACCEPT_ENCODING);
		if (ae != null && !ae.isEmpty()) {
			List<String> encodings = Arrays.asList(ae.split(","));
			List<String> filteredEncodings = encodings.stream().map(encoding -> encoding.replace(" ", ""))
					.filter(encoding -> !(SDCH_ENCODING.equalsIgnoreCase(encoding))).collect(Collectors.toList());
			headers.set(HttpHeaderNames.ACCEPT_ENCODING, String.join(",", filteredEncodings));
		}
	}

	/**
	 * Rewrite Location header according to requesting client
	 * 
	 * @param headers
	 */
	private void rewriteLocationHeader(ChannelHandlerContext ctx, HttpHeaders headers) {
		String originalUrlString = headers.getAsString(HttpHeaderNames.LOCATION);
		if (originalUrlString != null && !originalUrlString.isEmpty()) {
			try {
				HttpConfiguration config = (HttpConfiguration) ctx.channel().attr(TCPConstants.CONFIGURATION_KEY).get();
				String hostName = InetAddress.getLocalHost().getHostName();
				if (this.requestHost != null && !this.requestHost.isEmpty()) {
					hostName = this.requestHost.split(":")[0];
				}
				URL originalURL = new URL(originalUrlString);
				headers.remove(HttpHeaderNames.LOCATION);
				headers.add(HttpHeaderNames.LOCATION,
						new URL(originalURL.getProtocol(), hostName, config.getListenPort(), originalURL.getFile())
								.toString());
			} catch (UnknownHostException e) {
				LOGGER.error("Can't retrieve localhost ip", e);
			} catch (MalformedURLException e) {
				LOGGER.error("Can't rewrite url of a Location header", e);
			}
		}
	}

}
