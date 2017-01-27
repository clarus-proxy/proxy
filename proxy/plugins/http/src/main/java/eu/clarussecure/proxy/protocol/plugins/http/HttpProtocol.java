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

    /**
     * The Class Helper.
     */
    private static class Helper {

        /** The Constant CAPABILITIES. */
        private static final HttpCapabilities CAPABILITIES = new HttpCapabilities();

        /** The Constant CONFIGURATION. */
        private static final HttpConfiguration CONFIGURATION = new HttpConfiguration(CAPABILITIES);
    }

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
        Helper.CONFIGURATION.setListenPort(listenPort);
    }

    /*
     * (non-Javadoc)
     *
     * @see eu.clarussecure.proxy.spi.protocol.Protocol#getCapabilities()
     */
    @Override
    public ProtocolCapabilities getCapabilities() {
        return Helper.CAPABILITIES;
    }

    /*
     * (non-Javadoc)
     *
     * @see eu.clarussecure.proxy.spi.protocol.Protocol#getConfiguration()
     */
    @Override
    public HttpConfiguration getConfiguration() {
        return Helper.CONFIGURATION;
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
