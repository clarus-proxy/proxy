package eu.clarussecure.proxy.spi.security.policy;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class SecurityPolicy {

    public static final String DATA_ELT = "data";
    public static final String ATTRIBUTE_ELT = "attribute";
    public static final String NAME_ATTR = "name";
    public static final String PROTOCOL_ELT = "protocol";
    public static final String PLUGIN_ATTR = "plugin";
    public static final String PORT_ATTR = "listen";
    public static final String PROTECTION_ELT = "protection";
    public static final String MODULE_ATTR = "module";

    private final Document document;

    private SecurityPolicy(Document document) {
        this.document = document;
    }

    public static SecurityPolicy load(File securityPolicyFile)
            throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        OutputStreamWriter errorWriter = new OutputStreamWriter(System.err);
        db.setErrorHandler(new MyErrorHandler(new PrintWriter(errorWriter, true)));
        Document doc = db.parse(securityPolicyFile);
        return new SecurityPolicy(doc);
    }

    public Document getDocument() {
        return document;
    }

    public String[] getDataIds() {
        Node root = document.getFirstChild();
        Node data = findSubNode(DATA_ELT, root);
        List<Node> attributes = findSubNodes(ATTRIBUTE_ELT, data);
        if (attributes != null) {
            List<String> names = new ArrayList<>(attributes.size());
            for (Node attribute : attributes) {
                if (attribute != null) {
                    Node name = attribute.getAttributes().getNamedItem(NAME_ATTR);
                    if (name != null) {
                        names.add(name.getNodeValue());
                    }
                }
            }
            return names.toArray(new String[names.size()]);
        }
        return null;
    }

    public String getProtocolPluginName() {
        Node root = document.getFirstChild();
        Node protocol = findSubNode(PROTOCOL_ELT, root);
        String pluginName = null;
        if (protocol != null) {
            Node plugin = protocol.getAttributes().getNamedItem(PLUGIN_ATTR);
            if (plugin != null) {
                pluginName = plugin.getNodeValue();
            }
        }
        return pluginName;
    }

    public Integer getProtocolListenPort() {
        Node root = document.getFirstChild();
        Node protocol = findSubNode(PROTOCOL_ELT, root);
        Integer listenPort = null;
        if (protocol != null) {
            Node port = protocol.getAttributes().getNamedItem(PORT_ATTR);
            if (port != null) {
                String value = port.getNodeValue();
                if (value != null) {
                    listenPort = Integer.valueOf(value);
                }
            }
        }
        return listenPort;
    }

    public String getProtectionModuleName() {
        Node root = document.getFirstChild();
        Node protection = findSubNode(PROTECTION_ELT, root);
        String protectionModuleName = null;
        if (protection != null) {
            Node module = protection.getAttributes().getNamedItem(MODULE_ATTR);
            if (module != null) {
                protectionModuleName = module.getNodeValue();
            }
        }
        return protectionModuleName;
    }

    /**
     * Find the named subnode in a node's sublist.
     * <ul>
     * <li>Ignores comments and processing instructions.
     * <li>Ignores TEXT nodes (likely to exist and contain
     *         ignorable whitespace, if not validating.
     * <li>Ignores CDATA nodes and EntityRef nodes.
     * <li>Examines element nodes to find one with
     *        the specified name.
     * </ul>
     * @param name  the tag name for the element to find
     * @param node  the element node to start searching from
     * @return the Node found
     */
    public Node findSubNode(String name, Node node) {
        if (node.getNodeType() != Node.ELEMENT_NODE) {
            System.err.println("Error: Search node not of element type");
            System.exit(22);
        }

        if (! node.hasChildNodes()) return null;

        NodeList list = node.getChildNodes();
        for (int i=0; i < list.getLength(); i++) {
            Node subnode = list.item(i);
            if (subnode.getNodeType() == Node.ELEMENT_NODE) {
               if (subnode.getNodeName().equals(name))
                   return subnode;
            }
        }
        return null;
    }

    /**
     * Find the named subnode in a node's sublist.
     * <ul>
     * <li>Ignores comments and processing instructions.
     * <li>Ignores TEXT nodes (likely to exist and contain
     *         ignorable whitespace, if not validating.
     * <li>Ignores CDATA nodes and EntityRef nodes.
     * <li>Examines element nodes to find one with
     *        the specified name.
     * </ul>
     * @param name  the tag name for the element to find
     * @param node  the element node to start searching from
     * @return the Node found
     */
    public List<Node> findSubNodes(String name, Node node) {
        if (node.getNodeType() != Node.ELEMENT_NODE) {
            System.err.println("Error: Search node not of element type");
            System.exit(22);
        }

        if (! node.hasChildNodes()) return null;

        NodeList list = node.getChildNodes();
        List<Node> nodes = new ArrayList<>();
        for (int i=0; i < list.getLength(); i++) {
            Node subnode = list.item(i);
            if (subnode.getNodeType() == Node.ELEMENT_NODE) {
               if (subnode.getNodeName().equals(name))
                   nodes.add(subnode);
            }
        }
        return nodes;
    }

}
