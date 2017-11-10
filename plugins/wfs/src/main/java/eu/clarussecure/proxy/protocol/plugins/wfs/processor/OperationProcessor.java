package eu.clarussecure.proxy.protocol.plugins.wfs.processor;

import eu.clarussecure.proxy.spi.protocol.ProtocolService;
import io.netty.channel.ChannelHandlerContext;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerException;

public interface OperationProcessor {

    ProtocolService getProtocolService(ChannelHandlerContext ctx);

    void processOperation(ChannelHandlerContext ctx) throws XMLStreamException, JAXBException, TransformerException;

}
