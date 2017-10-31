package eu.clarussecure.proxy.protocol.plugins.wfs.processor;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.Operation;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.TransactionOperation;
import eu.clarussecure.proxy.protocol.plugins.wfs.util.xml.XMLEventStreamReader;
import eu.clarussecure.proxy.protocol.plugins.wfs.util.xml.XMLEventStreamWriter;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;
import io.netty.channel.ChannelHandlerContext;
import net.opengis.wfs.v_1_1_0.TransactionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.codehaus.staxmate.dom.DOMConverter;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class TransactionProcessor implements OperationProcessor {

    private static final String NAME_PATH_DELIMITER = "/";

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionProcessor.class);

    private XMLEventReader xmlEventReader;
    private XMLEventWriter xmlEventWriter;
    private DataOperation dataOperation;
    private static final JAXBContext TRANSACTION_JAXB_CONTEXT = initJAXBContext();
    private static final XMLEventFactory EVENT_FACTORY = XMLEventFactory.newInstance();

    public TransactionProcessor(XMLEventReader xmlEventReader, XMLEventWriter xmlEventWriter) {
        super();
        this.xmlEventReader = xmlEventReader;
        this.xmlEventWriter = xmlEventWriter;
    }

    @Override
    public ProtocolService getProtocolService(ChannelHandlerContext ctx) {
        Configuration configuration = ctx.channel().attr(TCPConstants.CONFIGURATION_KEY).get();
        return configuration.getProtocolService();
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
                String transactionOperationName = startElement.getName().getLocalPart();
                LOGGER.info("WFS Transation operation start --> " + transactionOperationName);
                TransactionOperation transactionOperation = TransactionOperation.fromValue(transactionOperationName);

                switch (transactionOperation) {
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

    /**
     * processInsertElement
     * @throws XMLStreamException
     * @throws TransformerException
     */
    private void processInsertElement() throws XMLStreamException, TransformerException {

        LOGGER.info("process insert Element");

        // XMLEvent currentEvent = null;
        DOMConverter domConverter = new DOMConverter();
        Document doc = domConverter.buildDocument(new XMLEventStreamReader(xmlEventReader));
        Node insertNode = doc.getFirstChild();

        Map<String, String> insertData = extractInsertData(insertNode);

        // TODO update Insert element with protected data here

        domConverter.writeFragment(insertNode, new XMLEventStreamWriter(xmlEventWriter, EVENT_FACTORY));

    }

    private void processUpdateElement() {

    }

    private void processDeleteElement() {

    }

    /**
     * extractInsertData
     * @param insertNode
     * @return
     * @throws TransformerException
     */
    private Map<String, String> extractInsertData(Node insertNode) throws TransformerException {

        Map<String, String> insertData = new HashMap<>();

        NodeList insertedFeatures = insertNode.getChildNodes();
        for (int i = 0; i < insertedFeatures.getLength(); i++) {
            Node feature = insertedFeatures.item(i);
            if (Node.ELEMENT_NODE == feature.getNodeType()) {
                extractFeatureData(insertData, feature);
            }
        }

        return insertData;
    }

    /**
     * extractFeatureData
     * @param data
     * @param feature
     * @throws TransformerException
     */
    private void extractFeatureData(Map<String, String> data, Node feature) throws TransformerException {

        String featureName = new QName(feature.getNamespaceURI(), feature.getLocalName()).toString();
        NodeList featureProperties = feature.getChildNodes();
        for (int j = 0; j < featureProperties.getLength(); j++) {
            Node property = featureProperties.item(j);
            if (Node.ELEMENT_NODE == property.getNodeType()) {
                // Use only the local name 'cause namespace isn't used by
                // geoserver
                String propertyName = property.getLocalName();
                data.put(featureName + NAME_PATH_DELIMITER + propertyName, extractNodeContentAsString(property));
            }
        }
    }

    /**
     * extractNodeContentAsString
     * @param property
     * @return
     * @throws TransformerException
     */
    private String extractNodeContentAsString(Node property) throws TransformerException {

        NodeList propertyValues = property.getChildNodes();
        Transformer t = TransformerFactory.newInstance().newTransformer();
        StringWriter propertyValueWriter = new StringWriter();
        StreamResult propertyValueStream = new StreamResult(propertyValueWriter);
        for (int i = 0; i < propertyValues.getLength(); i++) {
            Node propertyValue = propertyValues.item(i);
            t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            t.setOutputProperty(OutputKeys.INDENT, "yes");
            t.transform(new DOMSource(propertyValue), propertyValueStream);
        }
        return propertyValueWriter.toString().trim();
    }

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

}
