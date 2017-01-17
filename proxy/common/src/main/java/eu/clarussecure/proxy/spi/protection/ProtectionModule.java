package eu.clarussecure.proxy.spi.protection;

import org.w3c.dom.Document;

import eu.clarussecure.dataoperations.DataOperation;

public interface ProtectionModule {

    ProtectionModuleCapabilities getCapabilities();

    String getProtectionModuleName();

    DataOperation getDataOperation();

    void initialize(Document document, String[] dataIds);
}
