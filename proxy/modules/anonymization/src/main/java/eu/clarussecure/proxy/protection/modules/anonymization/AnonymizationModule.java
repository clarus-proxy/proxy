package eu.clarussecure.proxy.protection.modules.anonymization;

import java.util.Arrays;

import org.w3c.dom.Document;

import eu.clarussecure.dataoperations.DataOperation;
import eu.clarussecure.dataoperations.Operation;
import eu.clarussecure.dataoperations.Promise;
import eu.clarussecure.dataoperations.anonymization.AnonymizeModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;

public class AnonymizationModule implements ProtectionModule, DataOperation {

    private static final String PROTECTION_MODULE_NAME = "Anonymization";
    private AnonymizeModule anonymizeModule;

    private static class CapabilitiesHelper {
        private static final AnonymizationCapabilities INSTANCE = new AnonymizationCapabilities();
    }

    @Override
    public ProtectionModuleCapabilities getCapabilities() {
        return CapabilitiesHelper.INSTANCE;
    }

    @Override
    public String getProtectionModuleName() {
        return PROTECTION_MODULE_NAME;
    }

    @Override
    public void initialize(Document document) {
        anonymizeModule = new AnonymizeModule(document);
    }

    @Override
    public DataOperation getDataOperation() {
        return this;
    }

    @Override
    public Promise get(String[] attributeNames, String[] criteria, Operation operation) {
        // TODO workaround to fix type of geometry with coarsening
        int index = Arrays.asList(attributeNames).indexOf("AddGeometryColumn");
        if (index != -1) {
            criteria[index] = criteria[index].replaceAll("POINT", "POLYGON");
        }
        Promise promise = anonymizeModule.get(attributeNames, criteria, operation);
        if (promise == null && index != -1) {
            promise = new Promise() {
                
                @Override
                public String getOperation() {
                    return null;
                }
                
                @Override
                public int getId() {
                    return 0;
                }
                
                @Override
                public String[][] getCall() {
                    return null;
                }
                
                @Override
                public String[] getAttributeNames() {
                    return null;
                }
            };
        }
        return promise;
    }

    @Override
    public String[][] get(Promise promise, String[][] contents) {
        return anonymizeModule.get(promise, contents);
    }

    @Override
    public String[][] post(String[] attributeNames, String[][] contents) {
        return anonymizeModule.post(attributeNames, contents);
    }

    @Override
    public String[][] put(String[] attributeNames, String[] criteria, String[][] contents) {
        return anonymizeModule.put(attributeNames, criteria, contents);
    }

    @Override
    public void delete(String[] attributeNames, String[] criteria) {
        anonymizeModule.delete(attributeNames, criteria);
    }
}
