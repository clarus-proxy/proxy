package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.util.Arrays;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPServer;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;
import eu.clarussecure.proxy.spi.protocol.ProtocolExecutor;

public class PgsqlProtocol extends ProtocolExecutor {

    private final PgsqlCapabilities capabilities = new PgsqlCapabilities();
    private final PgsqlConfiguration configuration = new PgsqlConfiguration(capabilities);

    @Override
    public ProtocolCapabilities getCapabilities() {
        return capabilities;
    }

    @Override
    public PgsqlConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    protected TCPServer<FrontendSidePipelineInitializer, BackendSidePipelineInitializer> buildServer() {
        return new TCPServer<>(getConfiguration(), FrontendSidePipelineInitializer.class,
                BackendSidePipelineInitializer.class, 0);
    }

    @Override
    public String[] adaptDataIds(String[] dataIds) {
        // Add the public schema if any
        dataIds = Arrays.stream(dataIds).map(id -> getConfiguration().adaptDataId(id)).toArray(String[]::new);
        return dataIds;
    }
}
