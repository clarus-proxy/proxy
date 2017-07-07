package eu.clarussecure.proxy.protocol.plugins.wfs.handler;

import com.ctc.wstx.exc.WstxEOFException;
import com.ctc.wstx.exc.WstxParsingException;
import eu.clarussecure.proxy.protocol.plugins.wfs.exception.WfsParsingException;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsGetRequest;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsOperation;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsPostRequest;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsRequest;
import eu.clarussecure.proxy.protocol.plugins.wfs.processor.WfsGetRequestProcessor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLOutputFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;
import javax.xml.stream.*;
import javax.xml.stream.events.Attribute;
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

        ByteBuf buffer = Unpooled.buffer();

        FullHttpRequest request = null;
        HttpContent content = null;

        if (httpObject instanceof HttpRequest) {

            HttpRequest httpRequest = (HttpRequest) httpObject;
            WfsGetRequestProcessor eventProcessor = new WfsGetRequestProcessor();

            switch (httpRequest.getMethod().name()) {

            case "GET":
                LOGGER.info("this is a GET ");
                processGetRequest(ctx, httpRequest, eventProcessor);
                break;
            case "POST":
                LOGGER.info("this is a POST -- parsing the HTTP content");
                break;
            }

            ReferenceCountUtil.retain(httpObject);
            out.add(httpObject);

        }

        if (httpObject instanceof HttpContent) {

            HttpContent httpContent = (HttpContent) httpObject;

            String serviceAttribute = null;

            ByteBuf httpContentByteBuffer = httpContent.content();
            ByteBufInputStream requestInputStream = new ByteBufInputStream(httpContentByteBuffer);

            XMLEventReader xmlEventReader = xmlInputFactory.createXMLEventReader(requestInputStream);

            ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer);
            XMLEventWriter writer = xmlOutputFactory.createXMLEventWriter(outputStream);
            XMLEventFactory eventFactory = XMLEventFactory.newInstance();

            try {
                XMLEvent event = null;
                int eventType = 0;

                while (xmlEventReader.hasNext()) {

                    event = xmlEventReader.peek();

                    eventType = event.getEventType();
                    StartElement startElement;

                    /*
                    if (XMLEvent.START_ELEMENT == eventType) {
                        startElement = event.asStartElement();
                        if (serviceAttribute == null) {
                            serviceAttribute = checkProtocol(startElement);
                    
                        } else if (serviceAttribute.equals("WFS")) {
                            LOGGER.info(startElement.getName().getLocalPart());
                        } else {
                            throw new WfsParsingException("Not a WFS stream");
                        }
                    }
                    */
                    writer.add(event);
                    xmlEventReader.next();

                }

                writer.add(eventFactory.createEndDocument());

                content = new DefaultHttpContent(outputStream.buffer());

                ReferenceCountUtil.retain(content);
                out.add(content);

            } catch (WstxEOFException exception) {
                LOGGER.warn(
                        "unable to parse content with the StAX API. The payload of this request may not be an XML.");

            } catch (WstxParsingException exception) {
                LOGGER.warn("unable to parse content with the StAX API." + exception.getMessage());

            }
        }
    }

    private void processGetRequest(ChannelHandlerContext ctx, HttpRequest httpRequest,
            WfsGetRequestProcessor eventProcessor) throws WfsParsingException {

        try {
            QueryStringDecoder decoder = new QueryStringDecoder(httpRequest.uri());
            if (decoder.parameters().isEmpty()) {
                LOGGER.info("empty list");

            } else {

                WfsGetRequest request = new WfsGetRequest(httpRequest.protocolVersion(), httpRequest.method(),
                        httpRequest.uri());
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
            }

        } catch (WfsParsingException ex) {
            LOGGER.error(ex.toString());
        }

    }

    private String checkProtocol(StartElement element) throws WfsParsingException {

        String protocol = null;
        Attribute serviceAttribute = element.getAttributeByName(new QName("service"));
        if (serviceAttribute != null) {
            try {
                protocol = serviceAttribute.getValue().toUpperCase();
                LOGGER.info("service attribute = " + protocol);

            } catch (IllegalArgumentException e) {
                throw new WfsParsingException("Unknown protocol: " + protocol);
            }
        } else {
            throw new WfsParsingException("Missing service attribute in WFS root element");
        }
        return protocol;

    }

}
