package eu.clarussecure.proxy.protocol.plugins.wfs.processor;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.TransactionOperation;
import eu.clarussecure.proxy.protocol.plugins.wfs.util.xml.XMLEventStreamReader;
import eu.clarussecure.proxy.spi.*;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;
import io.netty.channel.ChannelHandlerContext;
import net.opengis.wfs.v_1_1_0.TransactionType;
import org.codehaus.staxmate.dom.DOMConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    /**
     * processOperation
     * @param ctx
     * @throws XMLStreamException
     */
    @Override
    public void processOperation(ChannelHandlerContext ctx) throws XMLStreamException, TransformerException {

        TransactionOperation transactionOperation = null;
        String transactionOperationName = "";

        // Extract module operation
        ModuleOperation moduleOperation = new OutboundDataOperation();

        // Qualify the transaction (Insert, Update, Delete)
        XMLEvent firstEvent = xmlEventReader.nextEvent();
        XMLEvent event = (XMLEvent) xmlEventReader.peek();

        if (XMLEvent.START_ELEMENT == event.getEventType()) {
            StartElement element = event.asStartElement();
            transactionOperationName = element.getName().getLocalPart();
            LOGGER.info("WFS Transaction operation start --> " + transactionOperationName);
        }

        Element element = parseTransactionXML(ctx);

        try {

            transactionOperation = TransactionOperation.fromValue(transactionOperationName);
            switch (transactionOperation) {
            case INSERT:
                moduleOperation = extractInsertOperation(ctx, element, (OutboundDataOperation) moduleOperation);
                break;
            case UPDATE:
                // moduleOperation = extractUpdateOperation(ctx, element, outboundDataOperation);
                break;
            case DELETE:
                // moduleOperation = extractDeleteOperation(ctx, element, outboundDataOperation);
                break;
            default:
                break;
            }

            if (moduleOperation instanceof OutboundDataOperation) {

                List<OutboundDataOperation> newOutboundDataOperations = newOutboundDataOperation(ctx,
                        (OutboundDataOperation) moduleOperation);

                boolean requestModified = newOutboundDataOperations.size() > 1;
                LOGGER.info(String.valueOf(requestModified));

                for (OutboundDataOperation newOutboundDataOperation : newOutboundDataOperations) {

                    if (transactionOperation.name().equals("INSERT")) {

                        Node newElement = modifyInsertNode(element, newOutboundDataOperation);

                    }
                }

            }

        } catch (IllegalArgumentException e) {
            LOGGER.info(e.getMessage());
        }

    }

    /**
     * extractInsertOperation
     * @param ctx
     * @param insertElement
     * @param outboundDataOperation
     * @return
     */
    private OutboundDataOperation extractInsertOperation(ChannelHandlerContext ctx, Element insertElement,
            OutboundDataOperation outboundDataOperation) {

        outboundDataOperation = new OutboundDataOperation();
        outboundDataOperation.setOperation(Operation.CREATE);

        String schemaId = "clarus";
        String datasetId = getDatasetId(ctx, insertElement, schemaId);

        // save the mapping between layer name and datasetid

        // extract data ids
        NodeList featureNodes = insertElement.getFirstChild().getChildNodes();

        List<CString> dataIds = nodeStream(featureNodes).map(Node::getNodeName) // get node name
                .map(StringUtilities::unquote) // unquote string
                .map(node -> datasetId + node) // build data id
                .map(CString::valueOf) // transform to CString
                .collect(Collectors.toList()); // build a list

        outboundDataOperation.setDataIds(dataIds);

        // save the mapping between attribute names and data ids

        // extract data values
        List<CString> dataValue = nodeStream(featureNodes).map(Node::getTextContent).map(CString::valueOf) // transform to CString
                .collect(Collectors.toList()); // build a list

        outboundDataOperation.addDataValue(dataValue);

        return outboundDataOperation;

    }

    /**
     * getDatasetId
     *      dataset is the layer
     * @param ctx
     * @param insertElement
     * @param schemaId
     * @return
     */
    private String getDatasetId(ChannelHandlerContext ctx, Element insertElement, String schemaId) {

        StringBuilder sb = new StringBuilder();
        Node layerNode = insertElement.getFirstChild();
        if (layerNode != null) {
            //sb.append(schemaId).append('/').append(layerNode.getLocalName().toString());
            sb.append(layerNode.getLocalName().toString());
        }
        sb.append('/');
        return sb.toString();
    }

    /**
     * nodeStream
     * @param list
     * @return
     */
    private static Stream<Node> nodeStream(NodeList list) {
        List<Node> nodes = new ArrayList<>();
        for (int n = 0; n < list.getLength(); ++n) {
            nodes.add(list.item(n));
        }
        return nodes.stream();
    }

    /**
     * parseTransactionXML
     * @param ctx
     * @return
     */
    private Element parseTransactionXML(ChannelHandlerContext ctx) throws XMLStreamException {

        Element element = null;

        DOMConverter domConverter = new DOMConverter();
        Document doc = domConverter.buildDocument(new XMLEventStreamReader(xmlEventReader));

        Node insertNode = doc.getFirstChild();

        // domConverter.writeFragment(newInsertNode, new XMLEventStreamWriter(xmlEventWriter, EVENT_FACTORY));

        return (Element) insertNode;
    }

    /**
     * modifyInsertNode
     * @param insertNode
     * @param outboundDataOperation
     * @return
     */
    private Node modifyInsertNode(Node insertNode, OutboundDataOperation outboundDataOperation)
            throws TransformerException {

        NodeList featureTypes = insertNode.getChildNodes();

        // 1. Update columns in clause into (if any)

        // 1.2 Retrieve protected data ids
        List<String> newDataIds1 = outboundDataOperation.getDataIds().stream().map(CString::toString)
                .collect(Collectors.toList());

        // 1.3 Retrieve the mapping between clear and protected data ids
        Map<CString, CString> mapping = outboundDataOperation.getDataIdMapping();

        // 1.4 Replace child nodes
        for (int i = 0; i < featureTypes.getLength(); i++) {
            Node feature = featureTypes.item(i);
            int index = 0;
            for (Map.Entry entry : mapping.entrySet()) {
                //LOGGER.info(entry.getKey().toString() + " : " + entry.getValue().toString());
                if (feature.getNodeType() == Node.ELEMENT_NODE) {
                    String featureName = new QName(feature.getLocalName()).toString();
                    NodeList featureProperties = feature.getChildNodes();

                    for (int j = 0; j < featureProperties.getLength(); j++) {
                        Node property = featureProperties.item(j);
                        // Use only the local name because namespace is not used by geoserver
                        String propertyName = property.getLocalName();
                        if (entry.getKey().toString().equals(featureName + NAME_PATH_DELIMITER + propertyName)) {
                            LOGGER.info("entry key is " + entry.getKey().toString());
                            Document doc = insertNode.getOwnerDocument();
                            /*
                            LOGGER.info("replace node " + propertyName + " with " + entry.getValue());
                            String value = entry.getValue().toString();
                            String rawval = value.substring(value.indexOf(NAME_PATH_DELIMITER) + 1);
                            if (doc != null) {
                                Element newproperty = (Element) doc.createElement(rawval);
                                insertNode.appendChild(newproperty);
                            
                            } else {
                                LOGGER.info("parent node exists");
                            }
                            */
                            property.setTextContent(outboundDataOperation.getDataValues().get(0).get(index).toString());
                            LOGGER.info("node value = " + property.getTextContent().toString());

                        }
                    }
                    index++;
                }
            }

        }

        return insertNode;

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
     *
     */
    private void processUpdateElement() {

    }

    /**
     *
     */
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
