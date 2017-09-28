package eu.clarussecure.proxy.protocol.plugins.wfs.handler;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;

import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLOutputFactory2;

import com.ctc.wstx.api.WstxInputProperties;

import eu.clarussecure.proxy.spi.buffer.QueueByteBufInputStream;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.util.concurrent.EventExecutorGroup;

public abstract class WfsDecoder extends MessageToMessageDecoder<HttpObject> {

    private static final int XML_PARSER_INPUT_BUF_SIZE = 128;

    protected XMLInputFactory xmlInputFactory;
    protected XMLOutputFactory xmlOutputFactory;
    protected EventExecutorGroup parserGroup;

    protected QueueByteBufInputStream currentContentStream;

    public WfsDecoder(EventExecutorGroup parserGroup) {
        super();
        this.parserGroup = parserGroup;
        this.xmlInputFactory = XMLInputFactory2.newInstance();
        this.xmlInputFactory.setProperty(WstxInputProperties.P_INPUT_BUFFER_LENGTH, XML_PARSER_INPUT_BUF_SIZE);
        this.xmlOutputFactory = XMLOutputFactory2.newInstance();
    }

    /**
     * Replace Content-Length header by Transfer-Encoding: chunked because
     * http-content is updated on the fly and streamed
     * 
     * @param headers
     */
    protected void replaceContentLengthByTransferEncodingHeader(HttpHeaders headers) {
        headers.remove(HttpHeaderNames.CONTENT_LENGTH);
        headers.remove(HttpHeaderNames.TRANSFER_ENCODING);
        headers.add(HttpHeaderNames.TRANSFER_ENCODING, "chunked");
    }

}
