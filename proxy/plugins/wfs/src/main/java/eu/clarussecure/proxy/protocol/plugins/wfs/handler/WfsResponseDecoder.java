package eu.clarussecure.proxy.protocol.plugins.wfs.handler;

import java.io.IOException;
import java.util.List;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.http.buffer.ChannelOutputStream;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.buffer.QueueByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * Created on 09/06/2017.
 */
public class WfsResponseDecoder extends WfsDecoder {

    private static final String NS_WFS = "http://www.opengis.net/wfs";
    private static final String PREFIX_WFS = "wfs";

    private static final Logger LOGGER = LoggerFactory.getLogger(WfsResponseDecoder.class);

    public WfsResponseDecoder(EventExecutorGroup parserGroup) {
        super(parserGroup);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject httpObject, List<Object> out) throws Exception {

        if (httpObject instanceof HttpResponse) {
            HttpResponse httpResponse = (HttpResponse) httpObject;
            this.replaceContentLengthByTransferEncodingHeader(httpResponse.headers());
            this.currentContentStream = null;
            ReferenceCountUtil.retain(httpObject);
            out.add(httpObject);
        }
        if (httpObject instanceof HttpContent) {
            final HttpContent httpContent = (HttpContent) httpObject;
            if (currentContentStream == null) {
                // First content part
                if (!httpContent.content().isReadable()) {
                    LOGGER.trace("Response contains no data");
                    ReferenceCountUtil.retain(httpContent);
                    out.add(httpContent);
                } else {
                    currentContentStream = new QueueByteBufInputStream(httpContent.content());
                    TCPSession session = ctx.channel().attr(TCPConstants.SESSION_KEY).get();
                    parserGroup.submit(() -> {
                        final ChannelOutputStream processedContentOutputStream = new ChannelOutputStream(ctx.alloc(),
                                session.getClientSideChannel());
                        try {
                            this.processContent(currentContentStream, processedContentOutputStream);
                        } catch (Exception e) {
                            LOGGER.error("Failed to process request content", e);
                        } finally {
                            try {
                                processedContentOutputStream.close();
                            } catch (IOException e) {
                                LOGGER.error("Can't close xml output stream", e);
                            }
                            try {
                                currentContentStream.close();
                            } catch (IOException e) {
                                LOGGER.error("Can't close xml output stream", e);
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

    /**
     * replaceContentLengthByTransferEncodingHeader
     * 
     * @param headers
     */
    protected void replaceContentLengthByTransferEncodingHeader(HttpHeaders headers) {

        headers.remove(HttpHeaderNames.CONTENT_LENGTH);
        headers.remove(HttpHeaderNames.TRANSFER_ENCODING);
        headers.add(HttpHeaderNames.TRANSFER_ENCODING, "chunked");
    }

    /**
     * newDataOperation
     * 
     * @param ctx
     * @param dataOperation
     * @return
     */
    private List<DataOperation> newDataOperation(ChannelHandlerContext ctx, DataOperation dataOperation) {
        /* TODO complete the code */
        return null;
    }

}
