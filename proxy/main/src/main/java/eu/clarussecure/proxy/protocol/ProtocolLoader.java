package eu.clarussecure.proxy.protocol;

import java.util.Iterator;
import java.util.ServiceLoader;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.Protocol;

public class ProtocolLoader {
    private static ProtocolLoader INSTANCE;
    private ServiceLoader<Protocol> loader;

    public static synchronized ProtocolLoader getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ProtocolLoader();
        }
        return INSTANCE;
    }

    private ProtocolLoader() {
        loader = ServiceLoader.load(Protocol.class);
    }

    public Protocol getProtocol(String protocolName) {
        Iterator<Protocol> protocolProviders = loader.iterator();
        while (protocolProviders.hasNext()) {
            Protocol protocol = protocolProviders.next();
            Configuration configuration = protocol.getConfiguration();
            if (protocolName.equalsIgnoreCase(configuration.getProtocolName())) {
                return protocol;
            }
        }
        return null;
    }
}
