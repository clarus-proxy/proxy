package eu.clarussecure.proxy.protocol.plugins.pgsql;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;

public class PgsqlConfiguration extends Configuration {

    public static final String PROTOCOL_NAME = "PostgreSQL";

    public static final int DEFAULT_LISTEN_PORT = 5432;

    public static final int DEFAULT_NB_OF_PARSER_THREADS = Runtime.getRuntime().availableProcessors();

    public PgsqlConfiguration(ProtocolCapabilities capabilities) {
        super(capabilities);
        nbParserThreads = DEFAULT_NB_OF_PARSER_THREADS;
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
