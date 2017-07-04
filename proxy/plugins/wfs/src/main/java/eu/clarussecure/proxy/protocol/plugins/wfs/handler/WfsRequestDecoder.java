package eu.clarussecure.proxy.protocol.plugins.wfs.handler;

import eu.clarussecure.proxy.protocol.plugins.wfs.exception.WfsParsingException;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsOperation;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsRequest;
import eu.clarussecure.proxy.protocol.plugins.wfs.processor.WfsGetRequestProcessor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLOutputFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.util.List;

/**
 * Created on 23/06/2017.
 */
public class WfsRequestDecoder extends MessageToMessageDecoder<HttpObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WfsResponseDecoder.class);

    private XMLInputFactory xmlInputFactory;
    private XMLOutputFactory xmlOutputFactory;

    public WfsRequestDecoder() {

        this.xmlInputFactory = XMLInputFactory2.newInstance();
        this.xmlOutputFactory = XMLOutputFactory2.newInstance();
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject httpObject, List<Object> out) throws Exception {

        if (httpObject instanceof HttpRequest) {

            HttpRequest httpRequest = (HttpRequest) httpObject;
            WfsGetRequestProcessor eventProcessor = new WfsGetRequestProcessor();

            /* TODO catch the WFS parsing exception */
            WfsRequest request = new WfsRequest(httpRequest.protocolVersion(), httpRequest.method(), httpRequest.uri());

            if (request.isWfsGetRequest()) {

                WfsOperation operation = request.getWfsOperation();

                LOGGER.info(String.format("Get Request Processor for %s operation", operation.getName()));

                switch (operation) {

                case GET_CAPABILITIES:
                    eventProcessor.processGetCapabilities(ctx, request);
                    break;
                case DESCRIBE_FEATURE_TYPE:
                    eventProcessor.processDescribeFeatureType(ctx, request);
                    break;
                case GET_FEATURE:
                    eventProcessor.processGetFeature(ctx, request);
                    break;
                }

            } else if (request.isWfsPostRequest()) {

                LOGGER.info("This is a POST request");

            }

        }
        if (httpObject instanceof HttpContent) {
            HttpContent content = (HttpContent) httpObject;

            ByteBuf httpContentByteBuffer = content.content();
            ByteBufInputStream requestInputStream = new ByteBufInputStream(httpContentByteBuffer);

            ByteBuf buffer = Unpooled.buffer();

            ByteBufOutputStream requestOutputStream = new ByteBufOutputStream(buffer);

            // process request
            XMLEventReader xmlEventReader = xmlInputFactory.createXMLEventReader(requestInputStream);
            XMLEventWriter xmlEventWriter = xmlOutputFactory.createXMLEventWriter(requestOutputStream);

            XMLEvent event = null;
            int eventType = 0;

            while (xmlEventReader.hasNext()) {
                event = xmlEventReader.peek();
                eventType = event.getEventType();
                StartElement startElement = null;
                if (XMLEvent.START_ELEMENT == eventType) {

                    startElement = event.asStartElement();
                    LOGGER.info(startElement.getName().getLocalPart());

                }
                xmlEventWriter.add(event);
                xmlEventReader.next();

            }

        }

        out.add(httpObject);
    }

}
