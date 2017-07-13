package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;
import eu.clarussecure.proxy.spi.security.policy.SecurityPolicy;

public class PgsqlConfiguration extends Configuration {

    public static final String PROTOCOL_NAME = "PostgreSQL";

    public static final int DEFAULT_PROTOCOL_PORT = 5432;

    public static final int DEFAULT_NB_OF_PARSER_THREADS = Runtime.getRuntime().availableProcessors();

    public static final String GEOMETRIC_DATA_ID = "dataId";

    public static final String GEOMETRIC_OBJECT_CLEAR_TYPE = "clearType";

    public static final String GEOMETRIC_OBJECT_PROTECTED_TYPE = "protectedType";

    private static final Pattern PUBLIC_DATA_ID_PATTERN = Pattern.compile("([^/]*/)(public\\.)?([^/\\.]*/[^/]*)");

    private static final String CSP_PATTERN = "csp\\d+";

    private static final String DATA_TECHNICAL_ID = "data_technical_id";

    private static final String GEOMETRIC_OBJECT_DEFINITION = "geometric_object_definition";

    private List<String> backendDatabaseNames;

    private String dataTechnicalId;

    private Map<String, String> geometryObjectDefinition;

    public PgsqlConfiguration(ProtocolCapabilities capabilities) {
        super(capabilities);
        nbParserThreads = DEFAULT_NB_OF_PARSER_THREADS;
    }

    @Override
    public String getProtocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public int getDefaultProtocolPort() {
        return DEFAULT_PROTOCOL_PORT;
    }

    @Override
    public void setParameters(SecurityPolicy securityPolicy) {
        super.setParameters(securityPolicy);
        String dataTechnicalId = null;
        String geometricDataId = null;
        String geometricObjectClearType = null;
        String geometricObjectProtectedType = null;
        Node rootNode = securityPolicy.getDocument().getFirstChild();
        Node dataNode = securityPolicy.findSubNode(SecurityPolicy.DATA_ELT, rootNode);
        List<Node> attributeNodes = securityPolicy.findSubNodes(SecurityPolicy.ATTRIBUTE_ELT, dataNode);
        if (attributeNodes != null) {
            for (Node attributeNode : attributeNodes) {
                Node nameAttr = attributeNode.getAttributes().getNamedItem(SecurityPolicy.NAME_ATTR);
                Node attributeTypeAttr = attributeNode.getAttributes().getNamedItem("attribute_type");
                if (nameAttr != null && attributeTypeAttr != null) {
                    String name = nameAttr.getNodeValue();
                    String attributeType = attributeTypeAttr.getNodeValue();
                    if ("technical_identifier".equals(attributeType)) {
                        dataTechnicalId = adaptDataId(name);
                    }
                }
                Node dataTypeAttr = attributeNode.getAttributes().getNamedItem("data_type");
                if (nameAttr != null && dataTypeAttr != null) {
                    String name = nameAttr.getNodeValue();
                    String dataType = dataTypeAttr.getNodeValue();
                    if ("geometric_object".equals(dataType)) {
                        geometricDataId = adaptDataId(name);
                    }
                }
            }
        }
        Node protectionNode = securityPolicy.findSubNode(SecurityPolicy.PROTECTION_ELT, rootNode);
        Node attributeTypesNode = securityPolicy.findSubNode("attribute_types", protectionNode);
        if (attributeTypesNode.hasChildNodes()) {
            NodeList subnodes = attributeTypesNode.getChildNodes();
            for (int i = 0; i < subnodes.getLength(); i++) {
                Node subnode = subnodes.item(i);
                if (subnode.getNodeType() == Node.ELEMENT_NODE) {
                    if (subnode.getNodeName().equals("attribute_type")) {
                        Node protectionAttr = subnode.getAttributes().getNamedItem("protection");
                        if (protectionAttr != null) {
                            String protection = protectionAttr.getNodeValue();
                            if ("splitting".equals(protection)) {
                                Node splittingTypeAttr = subnode.getAttributes().getNamedItem("splitting_type");
                                if (splittingTypeAttr != null) {
                                    String splittingType = splittingTypeAttr.getNodeValue();
                                    if ("points".equals(splittingType)) {
                                        geometricObjectClearType = GeometryType.POINT.toString();
                                        geometricObjectProtectedType = GeometryType.POINT.toString();
                                        break;
                                    } else if ("lines".equals(splittingType)) {
                                        geometricObjectClearType = GeometryType.POINT.toString();
                                        geometricObjectProtectedType = GeometryType.LINESTRING.toString();
                                        break;
                                    }
                                }
                            } else if ("coarsening".equals(protection)) {
                                geometricObjectClearType = GeometryType.POLYGON.toString();
                                geometricObjectProtectedType = GeometryType.POLYGON.toString();
                            }
                        }
                    }
                }
            }
        }
        Map<String, String> parameters = getParameters();
        if (parameters == null) {
            parameters = new HashMap<>();
            setParameters(parameters);
        }
        if (dataTechnicalId != null) {
            parameters.put(DATA_TECHNICAL_ID, dataTechnicalId);
        }
        StringBuilder sb = new StringBuilder();
        if (geometricDataId != null) {
            sb.append(GEOMETRIC_DATA_ID).append('=').append(geometricDataId).append(';');
        }
        if (geometricObjectClearType != null) {
            sb.append(GEOMETRIC_OBJECT_CLEAR_TYPE).append('=').append(geometricObjectClearType).append(';');
        }
        if (geometricObjectProtectedType != null) {
            sb.append(GEOMETRIC_OBJECT_PROTECTED_TYPE).append('=').append(geometricObjectProtectedType).append(';');
        }
        if (sb.length() > 0) {
            parameters.put(GEOMETRIC_OBJECT_DEFINITION, sb.toString());
        }
    }

    public String adaptDataId(String dataId/*, boolean fullyQualify*/) {
        dataId = dataId.indexOf('/') == -1
                // prepend with */*/ if there is no /
                ? "*/*/" + dataId
                : dataId.indexOf('/') == dataId.lastIndexOf('/')
                        // prepend with */ if there is one /
                        ? "*/" + dataId
                        // do nothing if there is two /
                        : dataId;
        Matcher m = PUBLIC_DATA_ID_PATTERN.matcher(dataId);
        if (m.matches()) {
            dataId = m.replaceAll("$1public.$3");
        }
        //        if (!fullyQualify) {
        //            dataId = dataId.startsWith("*/*/")
        //                    // remove */*/ if there is
        //                    ? dataId.substring("*/*/".length())
        //                    : dataId.startsWith("*/")
        //                            // remove */ if there is
        //                            ? dataId.substring("*/".length())
        //                            // else do nothing
        //                            : dataId;
        //        }
        return dataId;
    }

    public List<String> getBackendDatabaseNames() {
        if (backendDatabaseNames == null) {
            Map<String, String> parameters = getParameters();
            if (parameters != null) {
                Pattern cspPattern = Pattern.compile(CSP_PATTERN);
                backendDatabaseNames = parameters.entrySet().stream()
                        .filter(e -> cspPattern.matcher(e.getKey()).matches())
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> Arrays.stream(e.getValue().split(";")).map(p -> p.split("="))
                                        .filter(p -> "database".equals(p[0])).map(p -> p[1]).findFirst().orElse(null)))
                        .entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue)
                        .collect(Collectors.toList());
            } else {
                backendDatabaseNames = Collections.emptyList();
            }
        }
        return backendDatabaseNames;
    }

    public String getDataTechnicalId() {
        if (dataTechnicalId == null) {
            Map<String, String> parameters = getParameters();
            if (parameters != null) {
                dataTechnicalId = parameters.get(DATA_TECHNICAL_ID);
            }
        }
        return dataTechnicalId;
    }

    public Map<String, String> getGeometryObjectDefinition() {
        if (geometryObjectDefinition == null) {
            Map<String, String> parameters = getParameters();
            if (parameters != null) {
                String parameterValue = parameters.get(GEOMETRIC_OBJECT_DEFINITION);
                geometryObjectDefinition = Arrays.stream(parameterValue.split(";")).map(p -> p.split("="))
                        .collect(Collectors.toMap(tk -> tk[0], tk -> tk[1], (v1, v2) -> v2));
            } else {
                geometryObjectDefinition = Collections.emptyMap();
            }
        }
        return geometryObjectDefinition;
    }

}
