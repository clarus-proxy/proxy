package eu.clarussecure.proxy.protocol.plugins.wfs.handler;

import java.io.IOException;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.http.buffer.ChannelOutputStream;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;
import eu.clarussecure.proxy.protocol.plugins.wfs.exception.WfsParsingException;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsGetRequest;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsOperation;
import eu.clarussecure.proxy.protocol.plugins.wfs.processor.WfsGetRequestProcessor;
import eu.clarussecure.proxy.spi.buffer.QueueByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * Created on 23/06/2017.
 */
public class WfsRequestDecoder extends WfsDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(WfsResponseDecoder.class);

    public WfsRequestDecoder(EventExecutorGroup parserGroup) {
        super(parserGroup);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject httpObject, List<Object> out) throws Exception {

        if (httpObject instanceof HttpRequest) {

            HttpRequest httpRequest = (HttpRequest) httpObject;
            this.replaceContentLengthByTransferEncodingHeader(httpRequest.headers());
            this.currentContentStream = null;

            WfsGetRequestProcessor eventProcessor = new WfsGetRequestProcessor();
            switch (httpRequest.method().name()) {

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
            final HttpContent httpContent = (HttpContent) httpObject;
            if (currentContentStream == null) {
                // First content part
                if (!httpContent.content().isReadable()) {
                    LOGGER.trace("Request contains no data");
                    ReferenceCountUtil.retain(httpContent);
                    out.add(httpContent);
                } else {
                    currentContentStream = new QueueByteBufInputStream(httpContent.content());
                    TCPSession session = ctx.channel().attr(TCPConstants.SESSION_KEY).get();
                    parserGroup.submit(() -> {
                        final ChannelOutputStream requestOutputStream = new ChannelOutputStream(ctx.alloc(),
                                session.getServerSideChannel(0));
                        try {
                            // TODO replace with a real Request content
                            // processor
                            // (exemples are available in the old ows poc)
                            this.processContent(currentContentStream, requestOutputStream);
                        } catch (Exception e) {
                            LOGGER.error("Failed to process request content", e);
                        } finally {
                            try {
                                requestOutputStream.close();
                            } catch (IOException e) {
                                LOGGER.error("Can't close xml output stream", e);
                            }
                            try {
                                currentContentStream.close();
                            } catch (IOException e) {
                                LOGGER.error("Can't close xml input stream", e);
                            }
                        }
                    });
                }
            } else {
                currentContentStream.addBuffer(httpContent.content());
            }
            if (currentContentStream != null && httpContent instanceof LastHttpContent) {
                currentContentStream.addBuffer(QueueByteBufInputStream.END_OF_STREAMS);
            }
        }

    }

    private void processContent(QueueByteBufInputStream requestInputStream, ChannelOutputStream requestOutputStream)
            throws XMLStreamException {
        XMLEventReader xmlEventReader = xmlInputFactory.createXMLEventReader(requestInputStream);
        XMLEventWriter xmlEventWriter = xmlOutputFactory.createXMLEventWriter(requestOutputStream);

        XMLEvent event = null;
        int eventType = 0;
        while (xmlEventReader.hasNext()) {
            event = xmlEventReader.nextEvent();
            xmlEventWriter.add(event);

            eventType = event.getEventType();
            if (eventType == XMLEvent.START_ELEMENT) {
                StartElement rootElement = event.asStartElement();
                LOGGER.trace("OWS message root START ELEMENT --> " + rootElement.getName().toString());
                xmlEventWriter.flush();
            }
        }

        xmlEventWriter.flush();
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
