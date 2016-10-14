package eu.clarussecure.proxy.protection.modules.anonymization;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.w3c.dom.Document;

import eu.clarussecure.dataoperations.DataOperation;
import eu.clarussecure.dataoperations.Operation;
import eu.clarussecure.dataoperations.Promise;
import eu.clarussecure.dataoperations.anonymization.AnonymizeModule;
import eu.clarussecure.proxy.spi.protection.DefaultPromise;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;

public class AnonymizationModule implements ProtectionModule, DataOperation {

    private static final String PROTECTION_MODULE_NAME = "Anonymization";
    private AnonymizeModule anonymizeModule;
    private String[] dataIds;
    private Pattern[] dataIdPatterns;

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
    public void initialize(Document document, String[] dataIds) {
        anonymizeModule = new AnonymizeModule(document);
        this.dataIds = dataIds;
        if (this.dataIds != null) {
            this.dataIdPatterns = Arrays.stream(dataIds).map(s -> s.replace("*/", "(\\w*/)*")).map(Pattern::compile).toArray(Pattern[]::new);
        }
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
        // TODO workaround (promise not yet implemented in module) 
        //Promise promise = anonymizeModule.get(attributeNames, criteria, operation);
        DefaultPromise promise = new DefaultPromise();
        promise.setAttributeNames(attributeNames);
        return promise;
    }

    @Override
    public String[][] get(Promise promise, String[][] contents) {
        // TODO workaround (promise not yet implemented in module) 
        //return anonymizeModule.get(promise, contents);
        boolean modify = false;
        if (dataIdPatterns != null && promise != null && promise.getAttributeNames() != null) {
            modify = Arrays.stream(promise.getAttributeNames()).anyMatch(an -> Arrays.stream(dataIdPatterns).anyMatch(t -> t.matcher(an).matches()));
        }
        if (modify) {
            return contents.clone();
            //return Arrays.stream(contents).map(row -> Arrays.stream(row).map(v -> v + "*").toArray(String[]::new)).toArray(String[][]::new);
        }
        return contents;
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
