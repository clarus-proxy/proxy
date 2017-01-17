package eu.clarussecure.proxy.protocol.plugins.tcp;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;

public class TCPConfiguration extends Configuration {

    public static final String PROTOCOL_NAME = "TCP/IP";

    public static final int DEFAULT_LISTEN_PORT = -1;

    public TCPConfiguration(ProtocolCapabilities capabilities) {
        super(capabilities);
    }

    @Override
    public String getProtocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public int getDefaultListenPort() {
        return DEFAULT_LISTEN_PORT;
    }

}
