package eu.clarussecure.proxy.protocol.plugins.wfs.handler;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.TransformerException;

import eu.clarussecure.proxy.protocol.plugins.wfs.model.ProtocolVersion;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.WfsOperation;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.exception.OperationNotSupportedException;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.exception.ProtocolVersionNotSupportedException;
import eu.clarussecure.proxy.protocol.plugins.wfs.processor.OperationProcessor;
import eu.clarussecure.proxy.protocol.plugins.wfs.processor.factory.OperationProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.http.buffer.ChannelOutputStream;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;
import eu.clarussecure.proxy.protocol.plugins.wfs.parser.exception.WfsParsingException;
import eu.clarussecure.proxy.protocol.plugins.wfs.parser.message.WfsGetRequest;
import eu.clarussecure.proxy.protocol.plugins.wfs.processor.GetRequestProcessor;
import eu.clarussecure.proxy.spi.buffer.QueueByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutorGroup;

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

            switch (httpRequest.method().name()) {

            case "GET":
                // TODO use a factory for processors 
                processUrlParameters(ctx, httpRequest, new GetRequestProcessor());
                break;
            case "POST":
                // TODO check it is a WFS url
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
                            this.processContent(ctx, currentContentStream, requestOutputStream);

                        } catch (Exception e) {
                            LOGGER.error("Failed to process request content", e);
                        } finally {
                            try {
                                requestOutputStream.close();
                            } catch (IOException e) {
                                LOGGER.error("Cannot close xml output stream", e);
                            }
                            try {
                                currentContentStream.close();
                            } catch (IOException e) {
                                LOGGER.error("Cannot close xml input stream", e);
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

    /**
     * processContent
     * @param requestInputStream
     * @param requestOutputStream
     * @throws XMLStreamException
     */
    private void processContent(ChannelHandlerContext ctx, QueueByteBufInputStream requestInputStream,
            ChannelOutputStream requestOutputStream) throws XMLStreamException, ProtocolVersionNotSupportedException,
            OperationNotSupportedException, JAXBException, TransformerException {

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

                LOGGER.info("WFS message root START ELEMENT --> " + rootElement.getName().toString());

                ProtocolVersion protocolVersion = extractProtocolVersion(rootElement);
                String operationElName = rootElement.getName().getLocalPart();
                WfsOperation wfsOperation = WfsOperation.valueOfByName(operationElName);

                switch (protocolVersion) {

                case V1_0_0:
                    LOGGER.info("Process a WFS 1.0.0 request of type '{}'", wfsOperation);
                    break;
                case V1_1_0:
                    LOGGER.info("Process a WFS 1.1.0 request of type '{}'", wfsOperation);
                    OperationProcessor currentOperationProcessor = OperationProcessorFactory.getInstance()
                            .createOperationProcessor(wfsOperation, xmlEventReader, xmlEventWriter);
                    currentOperationProcessor.processOperation(ctx);

                    break;
                case V2_0_0:
                    LOGGER.info("Process a WFS 2.0.0 request of type '{}'", wfsOperation);
                    break;
                default:
                    throw new ProtocolVersionNotSupportedException(
                            "WFS Protocol version not supported: " + protocolVersion.getValue());
                }

                xmlEventWriter.flush();
            }
        }

        xmlEventWriter.flush();
    }

    /**
     * processGetRequest
     * @param ctx
     * @param httpRequest
     * @param eventProcessor
     * @throws WfsParsingException
     */
    private void processUrlParameters(ChannelHandlerContext ctx, HttpRequest httpRequest,
            GetRequestProcessor eventProcessor) throws WfsParsingException {

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

    /**
     * checkProtocol
     * @param element
     * @return
     * @throws WfsParsingException
     */
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

    /**
     * extractProtocolVersion
     * @param rootElement
     * @return
     * @throws ProtocolVersionNotSupportedException
     */
    private ProtocolVersion extractProtocolVersion(StartElement rootElement)
            throws ProtocolVersionNotSupportedException {

        ProtocolVersion protocolVersion = null;
        Attribute versionAttribute = rootElement.getAttributeByName(new QName("version"));
        if (versionAttribute != null) {
            String versionValue = versionAttribute.getValue();
            try {
                protocolVersion = ProtocolVersion.fromValue(versionValue);
            } catch (NoSuchElementException e) {
                throw new ProtocolVersionNotSupportedException(
                        "Unknown WFS protocol version:" + protocolVersion.getValue());
            }
        } else {
            throw new ProtocolVersionNotSupportedException("Missing version attribute in WFS message root element");
        }
        return protocolVersion;
    }

    /**
     * extractOperation
     * @param rootElement
     * @return
     */
    private WfsOperation extractOperation(StartElement rootElement) {
        // TODO extract operation from root element
        return WfsOperation.TRANSACTION;
    }

}
