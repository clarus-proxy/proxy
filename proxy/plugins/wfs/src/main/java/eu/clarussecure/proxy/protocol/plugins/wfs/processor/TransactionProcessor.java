package eu.clarussecure.proxy.protocol.plugins.wfs.processor;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.TransactionOperation;
import eu.clarussecure.proxy.protocol.plugins.wfs.util.xml.XMLEventStreamReader;
import eu.clarussecure.proxy.protocol.plugins.wfs.util.xml.XMLEventStreamWriter;
import eu.clarussecure.proxy.spi.*;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import net.opengis.wfs.v_1_1_0.TransactionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
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
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

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
    public void processOperation(ChannelHandlerContext ctx)
            throws XMLStreamException, JAXBException, TransformerException {

        XMLEvent firstEvent = xmlEventReader.nextEvent();

        ModuleOperation moduleOperation = null;

        /*
        if (!(firstEvent.getEventType() == XMLEvent.START_ELEMENT &&
                firstEvent.asStartElement().getName()
                .getLocalPart().equals(WfsOperation.TRANSACTION.getRequestRootElement()))) {
            throw new IllegalStateException("Bad XML stream parser state; must be START_ELEMENT 'Transaction'");
        }*/

        xmlEventWriter.add(firstEvent);
        xmlEventWriter.flush();
        XMLEvent event = null;
        int eventType = 0;
        StartElement startElement = null;

        while (xmlEventReader.hasNext()) {
            event = (XMLEvent) xmlEventReader.peek();
            //event = (XMLEvent) xmlEventReader.next();
            eventType = event.getEventType();

            if (XMLEvent.START_ELEMENT == eventType) {
                startElement = event.asStartElement();
                String transactionOperationName = startElement.getName().getLocalPart();
                LOGGER.info("WFS Transation operation start --> " + transactionOperationName);
                TransactionOperation transactionOperation = null;
                try {
                    transactionOperation = TransactionOperation.fromValue(transactionOperationName);
                    switch (transactionOperation) {
                    case INSERT:
                        processInsertElement(ctx);
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
                } catch (IllegalArgumentException e) {
                    LOGGER.info(e.getMessage());
                }
                xmlEventWriter.flush();

            } else {
                // TODO uncomment ?
                // xmlEventWriter.add(xmlEventReader.nextEvent());
            }
        }

    }

    // TODO integrate outbound data operation to specific WFS processing

    /**
     * processInsertElement
     * @throws XMLStreamException
     * @throws TransformerException
     */
    private void processInsertElement(ChannelHandlerContext ctx) throws XMLStreamException, TransformerException {

        LOGGER.info("process insert Element");

        ModuleOperation moduleOperation = extractInsertOperation(ctx, null);

        // XMLEvent currentEvent = null;
        DOMConverter domConverter = new DOMConverter();
        Document doc = domConverter.buildDocument(new XMLEventStreamReader(xmlEventReader));
        Node insertNode = doc.getFirstChild();

        // TODO update Insert element with protected data here
        // TODO remove cast for managing MetadataOperation
        moduleOperation = (OutboundDataOperation) moduleOperation;

        if (moduleOperation instanceof OutboundDataOperation) {

            OutboundDataOperation outboundDataOperation = (OutboundDataOperation) moduleOperation;
            List<OutboundDataOperation> newOutboundDataOperations = newOutboundDataOperation(ctx,
                    outboundDataOperation);

            if (newOutboundDataOperations.isEmpty()) {

            } else {

                List<Integer> involvedBackends;
                involvedBackends = new ArrayList<>(newOutboundDataOperations.size());

                for (OutboundDataOperation newOutboundDataOperation : newOutboundDataOperations) {

                    Node newInsertNode = modifyInsertNode(insertNode, newOutboundDataOperation);

                    domConverter.writeFragment(newInsertNode, new XMLEventStreamWriter(xmlEventWriter, EVENT_FACTORY));

                }
            }
        }

        // domConverter.writeFragment(insertNode, new XMLEventStreamWriter(xmlEventWriter, EVENT_FACTORY));

    }

    /**
     *
     * @param insertNode
     * @param newOutboundDataOperation
     * @return
     */
    private Node modifyInsertNode(Node insertNode, OutboundDataOperation newOutboundDataOperation)
            throws TransformerException {

        Node newInsertNode = insertNode;

        NodeList featureTypes = insertNode.getChildNodes();

        for (int i = 0; i < featureTypes.getLength(); i++) {

            Node feature = featureTypes.item(i);
            Node newfeature = feature;

            if (feature.getNodeType() == Node.ELEMENT_NODE) {

                String featureName = new QName(feature.getLocalName()).toString();
                NodeList featureProperties = feature.getChildNodes();

                for (int j = 0; j < featureProperties.getLength(); j++) {

                    Node property = featureProperties.item(j);
                    Node newproperty = property;

                    if (property.getNodeType() == Node.ELEMENT_NODE) {

                        // Use only the local name because namespace is not used by geoserver
                        String propertyName = property.getLocalName();

                        LOGGER.info(featureName + NAME_PATH_DELIMITER + propertyName,
                                extractNodeContentAsString(property));

                    }

                }
            }

        }

        return newInsertNode;

    }

    /**
     * newOutboundDataOperation
     * @param ctx
     * @param outboundDataOperation
     * @return
     */
    private List<OutboundDataOperation> newOutboundDataOperation(ChannelHandlerContext ctx,
            OutboundDataOperation outboundDataOperation) {

        List<OutboundDataOperation> newOutboundDataOperations = getProtocolService(ctx)
                .newOutboundDataOperation(outboundDataOperation);

        return newOutboundDataOperations;

    }

    /**
     * extractInsertOperation
     * @param ctx
     * @param outboundDataOperation
     * @return
     */
    private OutboundDataOperation extractInsertOperation(ChannelHandlerContext ctx,
            OutboundDataOperation outboundDataOperation) {

        if (outboundDataOperation == null) {
            outboundDataOperation = new OutboundDataOperation();
            outboundDataOperation.setOperation(Operation.CREATE);
        }

        // Extract dataset id

        // Extract data ids
        List<CString> dataIds = null;

        List<String> dataids = Arrays.asList("table_3857/address", "table_3857/geom");

        dataIds = dataids.stream().map(CString::valueOf)
                // build a list
                .collect(Collectors.toList());
        outboundDataOperation.setDataIds(dataIds);

        // Extract data values
        List<CString> dataValue = null;

        List<String> dataval = Arrays.asList("10 rue Gallieni", "POINT(794967.088894511 5452372.66919852)");
        dataValue = dataval.stream().map(CString::valueOf).map(value -> {
            return value;
        })
                // build a list
                .collect(Collectors.toList());

        outboundDataOperation.addDataValue(dataValue);

        return outboundDataOperation;

    }

    private void processUpdateElement() {

    }

    private void processDeleteElement() {

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
