package eu.clarussecure.proxy.protection;

import java.util.Iterator;
import java.util.ServiceLoader;

import eu.clarussecure.proxy.spi.protection.ProtectionModule;

public class ProtectionModuleLoader {
    private static ProtectionModuleLoader INSTANCE;
    private ServiceLoader<ProtectionModule> loader;

    public static synchronized ProtectionModuleLoader getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ProtectionModuleLoader();
        }
        return INSTANCE;
    }
 
    private ProtectionModuleLoader() {
        loader = ServiceLoader.load(ProtectionModule.class);
    }
 
    public ProtectionModule getProtectionModule(String protectionModuleName) {
        Iterator<ProtectionModule> protectionModuleProviders = loader.iterator();
        while (protectionModuleProviders.hasNext()) {
            ProtectionModule protectionModule = protectionModuleProviders.next();
            if (protectionModuleName.equalsIgnoreCase(protectionModule.getProtectionModuleName())) {
                return protectionModule;
            }
        }
        return null;
    }
 }
