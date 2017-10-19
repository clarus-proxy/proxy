/*
 * 
 */
package eu.clarussecure.proxy.protocol.plugins.http.handler.codec;

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
import eu.clarussecure.proxy.protocol.plugins.http.HttpSession;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

// TODO: Auto-generated Javadoc
/**
 * The Class HttpHeaderCodec.
 */
public class HttpHeaderCodec extends MessageToMessageCodec<HttpRequest, HttpResponse> {

    /** The Constant SDCH_ENCODING. */
    private static final String SDCH_ENCODING = "sdch";

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpHeaderCodec.class);

    /** The request host. */
    private String requestHost;

    /**
     * Instantiates a new http header codec.
     */
    public HttpHeaderCodec() {
        super();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * io.netty.handler.codec.MessageToMessageCodec#encode(io.netty.channel.
     * ChannelHandlerContext, java.lang.Object, java.util.List)
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, HttpResponse response, List<Object> out) throws Exception {
        // rewriteLocationHeader(ctx, response);
        ReferenceCountUtil.retain(response);
        out.add(response);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * io.netty.handler.codec.MessageToMessageCodec#decode(io.netty.channel.
     * ChannelHandlerContext, java.lang.Object, java.util.List)
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, HttpRequest request, List<Object> out) throws Exception {
        if (request.method().equals(HttpMethod.CONNECT)) {
            HttpResponse response = new DefaultHttpResponse(request.protocolVersion(), HttpResponseStatus.OK);
            HttpSession httpSession = (HttpSession) ctx.channel().attr(TCPConstants.SESSION_KEY).get();
            httpSession.getClientSideChannel().writeAndFlush(response);
        } else {
            // rewriteHostHeader(ctx, request);
            // removeSDCHEncoding(request);
            ReferenceCountUtil.retain(request);
            out.add(request);
        }
    }

    /**
     * Rewrite Host header according to targeted server.
     *
     * @param ctx
     *            the ctx
     * @param request
     *            the request
     */
    private void rewriteHostHeader(ChannelHandlerContext ctx, HttpRequest request) {
        HttpHeaders headers = request.headers();
        HttpConfiguration config = (HttpConfiguration) ctx.channel().attr(TCPConstants.CONFIGURATION_KEY).get();
        this.requestHost = headers.get(HttpHeaderNames.HOST);
        if (this.requestHost != null && !this.requestHost.isEmpty() && config.getServerEndpoint() != null) {
            headers.remove(HttpHeaderNames.HOST);
            headers.add(HttpHeaderNames.HOST, config.getServerEndpoint().getAddress().getHostAddress() + ":"
                    + config.getServerEndpoint().getPort());
        }
    }

    /**
     * Remove sdch from encodings we accept since we can't decode it.
     *
     * @param request
     *            the request
     */
    private void removeSDCHEncoding(HttpRequest request) {
        HttpHeaders headers = request.headers();
        String ae = headers.get(HttpHeaderNames.ACCEPT_ENCODING);
        if (ae != null && !ae.isEmpty()) {
            List<String> encodings = Arrays.asList(ae.split(","));
            List<String> filteredEncodings = encodings.stream().map(encoding -> encoding.replace(" ", ""))
                    .filter(encoding -> !(SDCH_ENCODING.equalsIgnoreCase(encoding))).collect(Collectors.toList());
            headers.set(HttpHeaderNames.ACCEPT_ENCODING, String.join(",", filteredEncodings));
        }
    }

    /**
     * Rewrite Location header according to requesting client.
     *
     * @param ctx
     *            the ctx
     * @param response
     *            the response
     */
    private void rewriteLocationHeader(ChannelHandlerContext ctx, HttpResponse response) {
        HttpHeaders headers = response.headers();
        this.requestHost = headers.get(HttpHeaderNames.HOST);
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
