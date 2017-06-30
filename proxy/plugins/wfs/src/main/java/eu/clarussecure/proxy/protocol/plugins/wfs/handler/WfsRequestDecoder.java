package eu.clarussecure.proxy.protocol.plugins.wfs.handler;

import eu.clarussecure.proxy.protocol.plugins.wfs.exception.WfsParsingException;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsOperation;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsRequest;
import eu.clarussecure.proxy.protocol.plugins.wfs.processor.WfsGetRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created on 23/06/2017.
 */
public class WfsRequestDecoder extends MessageToMessageDecoder<HttpObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WfsResponseDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject httpObject, List<Object> out) throws Exception {

        if (httpObject instanceof HttpRequest) {

            HttpRequest request = (HttpRequest) httpObject;

            WfsGetRequestProcessor eventProcessor = new WfsGetRequestProcessor();

            try {
                if (request.uri().indexOf("&") != -1) {

                    WfsRequest wfsRequest = new WfsRequest(request.protocolVersion(), request.method(), request.uri());
                    WfsOperation operation = wfsRequest.getWfsOperation();
                    WfsRequest newWfsRequest;

                    LOGGER.info(String.format("Get Request Processor for %s operation", operation.getName()));

                    switch (operation) {

                    case GET_CAPABILITIES:
                        eventProcessor.processGetCapabilities(ctx, wfsRequest);
                        break;
                    case DESCRIBE_FEATURE_TYPE:
                        eventProcessor.processDescribeFeatureType(ctx, wfsRequest);
                        break;
                    case GET_FEATURE:
                        eventProcessor.processGetFeature(ctx, wfsRequest);
                        break;
                    }
                }

            } catch (WfsParsingException ex) {
                LOGGER.error("WFS parsing exception");

            }

        }
        if (httpObject instanceof HttpContent) {
            HttpContent content = (HttpContent) httpObject;
        }

        out.add(httpObject);
    }

}
