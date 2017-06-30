package eu.clarussecure.proxy.protocol.plugins.wfs.handler;

import eu.clarussecure.proxy.spi.DataOperation;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLOutputFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.*;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.FileOutputStream;
import java.util.List;

/**
 * Created on 09/06/2017.
 */
public class WfsResponseDecoder extends MessageToMessageDecoder<HttpObject> {

    private XMLInputFactory xmlInputFactory;
    private XMLOutputFactory xmlOutputFactory;

    private static final String NS_WFS = "http://www.opengis.net/wfs";
    private static final String PREFIX_WFS = "wfs";

    private static final Logger LOGGER = LoggerFactory.getLogger(WfsResponseDecoder.class);

    public WfsResponseDecoder() {

        this.xmlInputFactory = XMLInputFactory2.newInstance();
        this.xmlOutputFactory = XMLOutputFactory2.newInstance();

    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject httpObject, List<Object> out) throws Exception {

        if (httpObject instanceof HttpResponse) {
            HttpResponse httpResponse = (HttpResponse) httpObject;
            this.replaceContentLengthByTransferEncodingHeader(httpResponse.headers());

        }
        if (httpObject instanceof HttpContent) {

            final HttpContent httpContent = (HttpContent) httpObject;

            ByteBuf httpContentByteBuffer = httpContent.content();

            ByteBufInputStream byteBufInputStream = new ByteBufInputStream(httpContentByteBuffer);

            XMLEventReader xmlEventReader = xmlInputFactory.createXMLEventReader(byteBufInputStream);

            // test with a file output stream
            FileOutputStream outputStream = new FileOutputStream("response.xml");

            XMLEventWriter writer = xmlOutputFactory.createXMLEventWriter(outputStream);
            XMLEventFactory eventFactory = XMLEventFactory.newInstance();

            try {

                XMLEvent event = null;
                int eventType = 0;

                while (xmlEventReader.hasNext()) {

                    event = xmlEventReader.peek();
                    eventType = event.getEventType();
                    StartElement startElement = null;
                    if (XMLEvent.START_ELEMENT == eventType) {

                        startElement = event.asStartElement();
                        LOGGER.info(startElement.getName().getLocalPart());
                        if (startElement.getName().getLocalPart().equals("featureMember")) {

                            // marshalling/unmarshalling via JAX
                            // JAXBContext context = JAXBContext.newInstance(WfsResponseDecoder.class);
                            // Unmarshaller unmarshaller = context.createUnmarshaller();
                            // unmarshaller.unmarshal(xmlEventReader, FeatureCollectionType.class);

                        }
                    }
                    writer.add(event);
                    xmlEventReader.next();
                }

                writer.add(eventFactory.createEndDocument());

            } catch (XMLStreamException e) {
                LOGGER.error("Error during WFS message processing", e);

            } finally {
                try {

                    outputStream.close();
                    // writer.flush();
                    // writer.close();

                } catch (Exception e) {
                    LOGGER.error("Can't close xml writer", e);
                }
            }
            if (httpContent instanceof LastHttpContent) {
                System.out.println("LAST HTTP CONTENT");
            }
        }

        ReferenceCountUtil.retain(httpObject);
        out.add(httpObject);

    }

    /**
     * replaceContentLengthByTransferEncodingHeader
     * @param headers
     */
    protected void replaceContentLengthByTransferEncodingHeader(HttpHeaders headers) {

        headers.remove(HttpHeaderNames.CONTENT_LENGTH);
        headers.remove(HttpHeaderNames.TRANSFER_ENCODING);
        headers.add(HttpHeaderNames.TRANSFER_ENCODING, "chunked");
    }

    /**
     * newDataOperation
     * @param ctx
     * @param dataOperation
     * @return
     */
    private List<DataOperation> newDataOperation(ChannelHandlerContext ctx, DataOperation dataOperation) {
        return null;
    }

}
