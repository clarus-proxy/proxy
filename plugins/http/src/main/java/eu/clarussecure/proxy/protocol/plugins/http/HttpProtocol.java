/*
 *
 */
package eu.clarussecure.proxy.protocol.plugins.http;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPServer;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;
import eu.clarussecure.proxy.spi.protocol.ProtocolExecutor;

// TODO: Auto-generated Javadoc
/**
 * The Class HttpProtocol.
 */
public class HttpProtocol extends ProtocolExecutor {

    private final HttpCapabilities capabilities = new HttpCapabilities();

    private final HttpConfiguration configuration = new HttpConfiguration(capabilities);

    /**
     * Instantiates a new http protocol.
     */
    public HttpProtocol() {

    }

    /**
     * Instantiates a new http protocol.
     *
     * @param listenPort
     *            the listen port
     */
    public HttpProtocol(int listenPort) {
        configuration.setListenPort(listenPort);
    }

    /*
     * (non-Javadoc)
     *
     * @see eu.clarussecure.proxy.spi.protocol.Protocol#getCapabilities()
     */
    @Override
    public ProtocolCapabilities getCapabilities() {
        return capabilities;
    }

    /*
     * (non-Javadoc)
     *
     * @see eu.clarussecure.proxy.spi.protocol.Protocol#getConfiguration()
     */
    @Override
    public HttpConfiguration getConfiguration() {
        return configuration;
    }

    /*
     * (non-Javadoc)
     *
     * @see eu.clarussecure.proxy.spi.protocol.ProtocolExecutor#buildServer()
     */
    @Override
    protected TCPServer<HttpClientPipelineInitializer, HttpServerPipelineInitializer> buildServer() {
        return new TCPServer<>(getConfiguration(), HttpClientPipelineInitializer.class,
                HttpServerPipelineInitializer.class, 0);
    }

}
