package eu.clarussecure.proxy.protocol.plugins.wfs.processor;

import eu.clarussecure.proxy.protocol.plugins.wfs.model.Operation;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.TransactionOperation;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;
import io.netty.channel.ChannelHandlerContext;
import net.opengis.wfs.v_1_1_0.TransactionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.TransformerException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class TransactionProcessor implements OperationProcessor {

    private static final String NAME_PATH_DELIMITER = "/";

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionProcessor.class);

    private XMLEventReader xmlEventReader;
    private XMLEventWriter xmlEventWriter;
    private DataOperation dataOperation;
    private static final JAXBContext TRANSACTION_JAXB_CONTEXT = initJAXBContext();
    private static final XMLEventFactory EVENT_FACTORY = XMLEventFactory.newInstance();

    /**
     * initJAXBContext
     * @return
     */
    private static JAXBContext initJAXBContext() {
        JAXBContext staticInstance = null;
        try {
            staticInstance = JAXBContext.newInstance(TransactionType.class);
        } catch (JAXBException e) {
            throw new RuntimeException("Cannot init jaxb context", e);
        }
        return staticInstance;
    }

    @Override
    public ProtocolService getProtocolService(ChannelHandlerContext ctx) {
        return null;
    }

    @Override
    public void processOperation() throws XMLStreamException, JAXBException, TransformerException {

        XMLEvent firstEvent = xmlEventReader.nextEvent();

        /*
        if (!(firstEvent.getEventType() == XMLEvent.START_ELEMENT &&
                firstEvent.asStartElement().getName()
                .getLocalPart().equals(Operation.TRANSACTION.getRequestRootElement()))) {
            throw new IllegalStateException("Bad XML stream parser state; must be START_ELEMENT 'Transaction'");
        }*/

        xmlEventWriter.add(firstEvent);
        xmlEventWriter.flush();
        XMLEvent event = null;
        int eventType = 0;
        StartElement startElement = null;
        while (xmlEventReader.hasNext()) {
            event = (XMLEvent) xmlEventReader.peek();
            eventType = event.getEventType();
            if (XMLEvent.START_ELEMENT == eventType) {
                startElement = event.asStartElement();
                String transactionSubOperationName = startElement.getName().getLocalPart();
                LOGGER.trace("WFS Transaction SUB_OPERATION start --> " + transactionSubOperationName);
                TransactionOperation subOperation = TransactionOperation.fromValue(transactionSubOperationName);
                switch (subOperation) {
                case INSERT:
                    processInsertElement();
                    break;
                case UPDATE:
                    processUpdateElement();
                    break;
                case DELETE:
                    processDeleteElement();
                    break;
                default:
                    break;
                }
                xmlEventWriter.flush();
            } else {
                xmlEventWriter.add(xmlEventReader.nextEvent());
            }
        }
    }

    private void processInsertElement() throws XMLStreamException, TransformerException {
        LOGGER.info("process insert Element");

    }

    private void processUpdateElement() {

    }

    private void processDeleteElement() {

    }

}
