package eu.clarussecure.proxy.protocol.plugins.wfs;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPServer;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;
import eu.clarussecure.proxy.spi.protocol.ProtocolExecutor;

/**
 * Created on 12/06/2017.
 */
public class WfsProtocol extends ProtocolExecutor {

    private static class Helper {
        private static final WfsCapabilities CAPABILITIES = new WfsCapabilities();
        private static final WfsConfiguration CONFIGURATION = new WfsConfiguration(CAPABILITIES);
    }

    @Override
    protected TCPServer<WfsClientPipelineInitializer, WfsServerPipelineInitializer> buildServer() {
        return new TCPServer<>(getConfiguration(), WfsClientPipelineInitializer.class,
                WfsServerPipelineInitializer.class, 0);
    }

    @Override
    public ProtocolCapabilities getCapabilities() {
        return WfsProtocol.Helper.CAPABILITIES;
    }

    @Override
    public Configuration getConfiguration() {
        return WfsProtocol.Helper.CONFIGURATION;
    }

}
