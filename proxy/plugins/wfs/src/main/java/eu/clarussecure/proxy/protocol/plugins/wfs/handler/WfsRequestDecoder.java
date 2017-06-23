package eu.clarussecure.proxy.protocol.plugins.wfs.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;

import java.util.List;

/**
 * Created by administrateur on 23/06/2017.
 */
public class WfsRequestDecoder extends MessageToMessageDecoder<HttpObject> {

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, HttpObject httpObject, List<Object> out)
            throws Exception {

        if (httpObject instanceof HttpRequest) {
            HttpRequest httpRequest = (HttpRequest) httpObject;

        }
        if (httpObject instanceof HttpContent) {

            final HttpContent httpContent = (HttpContent) httpObject;

        }

        out.add(httpObject);

    }
}
