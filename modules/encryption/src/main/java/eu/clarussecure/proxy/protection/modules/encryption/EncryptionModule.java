package eu.clarussecure.proxy.protection.modules.encryption;

import org.w3c.dom.Document;

import eu.clarussecure.dataoperations.DataOperation;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;

public class EncryptionModule implements ProtectionModule {

    private static final String PROTECTION_MODULE_NAME = "Encryption";

    private eu.clarussecure.dataoperations.encryption.EncryptionModule encryptionModule;

    private static class CapabilitiesHelper {
        private static final EncryptionCapabilities INSTANCE = new EncryptionCapabilities();
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
        encryptionModule = new eu.clarussecure.dataoperations.encryption.EncryptionModule(document);
    }

    @Override
    public DataOperation getDataOperation() {
        return encryptionModule;
    }

}
