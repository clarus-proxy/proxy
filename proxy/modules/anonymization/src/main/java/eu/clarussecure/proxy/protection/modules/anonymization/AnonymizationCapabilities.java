package eu.clarussecure.proxy.protection.modules.anonymization;

import java.util.Map;
import java.util.Set;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import eu.clarussecure.proxy.spi.Capabilities;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;
import eu.clarussecure.proxy.spi.security.policy.SecurityPolicy;

public class AnonymizationCapabilities implements ProtectionModuleCapabilities {

    private final Map<Operation, Set<Mode>> datasetCRUDOperations = Capabilities.toMap(new Enum<?>[][] {
        {Operation.CREATE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING},
        {Operation.READ, Mode.AS_IT_IS},
        {Operation.UPDATE},
        {Operation.DELETE, Mode.AS_IT_IS}});

    private final Map<Operation, Set<Mode>> recordCRUDOperations = Capabilities.toMap(new Enum<?>[][] {
        {Operation.CREATE},
        {Operation.READ, Mode.AS_IT_IS},
        {Operation.UPDATE},
        {Operation.DELETE}});

    @Override
    public Set<Operation> getSupportedCRUDOperations(boolean wholeDataset) {
        return wholeDataset ? datasetCRUDOperations.keySet() : recordCRUDOperations.keySet();
    }

    @Override
    public Set<Mode> getSupportedProcessingModes(boolean wholeDataset, Operation operation) {
        return wholeDataset ? datasetCRUDOperations.get(operation) : recordCRUDOperations.get(operation);
    }

    @Override
    public Mode getPreferredProcessingMode(boolean wholedataset, Operation operation, SecurityPolicy securityPolicy) {
        Set<Mode> modes = getSupportedProcessingModes(wholedataset, operation);
        if (modes.size() <= 1) {
            return modes.isEmpty() ? null : modes.iterator().next();
        }
        Mode preferredMode = Mode.AS_IT_IS;
        if (wholedataset && operation == Operation.CREATE) {
            Node rootNode = securityPolicy.getDocument().getFirstChild();
            Node protectionNode = securityPolicy.findSubNode(SecurityPolicy.PROTECTION_ELT, rootNode);
            Node attributeTypesNode = securityPolicy.findSubNode("attribute_types", protectionNode);
            if (attributeTypesNode.hasChildNodes()) {
                //preferredMode = Mode.STREAMING;
                preferredMode = Mode.BUFFERING;
                NodeList subnodes = attributeTypesNode.getChildNodes();
                for (int i=0; i < subnodes.getLength(); i++) {
                    Node subnode = subnodes.item(i);
                    if (subnode.getNodeType() == Node.ELEMENT_NODE) {
                       if (subnode.getNodeName().equals("attribute_type")) {
                           Node protectionAttr = subnode.getAttributes().getNamedItem("protection");
                           if (protectionAttr != null) {
                               String protection = protectionAttr.getNodeValue();
                               if ("coarsening".equals(protection)) {
                                   Node coarseningTypeAttr = subnode.getAttributes().getNamedItem("coarsening_type");
                                   if (coarseningTypeAttr != null) {
                                       String coarseningType = coarseningTypeAttr.getNodeValue();
                                       if ("microaggregation".equals(coarseningType)) {
                                           preferredMode = Mode.BUFFERING;
                                           break;
                                       }
                                   }
                               }
                           }
                       }
                    }
                }
            }
        }
        return preferredMode;
    }
}
