package eu.clarussecure.proxy.protection.modules.splitting;

import org.w3c.dom.Document;

import eu.clarussecure.dataoperations.DataOperation;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;

public class SplittingModule implements ProtectionModule {

    private static final String PROTECTION_MODULE_NAME = "Splitting";

    private eu.clarussecure.dataoperations.splitting.SplittingModule splittingModule;

    private static class CapabilitiesHelper {
        private static final SplittingCapabilities INSTANCE = new SplittingCapabilities();
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
        splittingModule = new eu.clarussecure.dataoperations.splitting.SplittingModule(document);
    }

    @Override
    public DataOperation getDataOperation() {
        return splittingModule;
    }

}
