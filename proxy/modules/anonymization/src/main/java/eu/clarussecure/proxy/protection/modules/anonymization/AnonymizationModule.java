package eu.clarussecure.proxy.protection.modules.anonymization;

import org.w3c.dom.Document;

import eu.clarussecure.dataoperations.DataOperation;
import eu.clarussecure.dataoperations.anonymization.AnonymizeModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;

public class AnonymizationModule implements ProtectionModule {

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
    public void initialize(Document document, String[] dataIds, String[] datasetPrefixByServer) {
        anonymizeModule = new AnonymizeModule(document);
    }

    @Override
    public DataOperation getDataOperation() {
        return anonymizeModule;

    }

}
