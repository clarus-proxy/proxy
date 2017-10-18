package eu.clarussecure.proxy.protection.modules.encryption;

import org.w3c.dom.Document;

import eu.clarussecure.dataoperations.DataOperation;
import eu.clarussecure.dataoperations.SEmodule.SearchableEncryptionModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;

public class SEModule implements ProtectionModule {

    private static final String PROTECTION_MODULE_NAME = "Searchable encryption";

    private SearchableEncryptionModule encryptionModule;

    private static class CapabilitiesHelper {
        private static final SECapabilities INSTANCE = new SECapabilities();
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
        encryptionModule = new SearchableEncryptionModule(document);
    }

    @Override
    public DataOperation getDataOperation() {
        return encryptionModule;
    }

}
