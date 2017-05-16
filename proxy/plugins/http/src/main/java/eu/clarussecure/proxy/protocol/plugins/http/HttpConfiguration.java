/*
 * 
 */
package eu.clarussecure.proxy.protocol.plugins.http;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;

// TODO: Auto-generated Javadoc
/**
 * The Class HttpConfiguration.
 */
public class HttpConfiguration extends Configuration {

	/** The Constant PROTOCOL_NAME. */
	private static final String PROTOCOL_NAME = "HTTP/1.1";
	
	/** The Constant DEFAULT_PROTOCOL_PORT. */
	private static final int DEFAULT_PROTOCOL_PORT = 80;
	
	/** The Constant DEFAULT_CLIENT_SSL_PORT. */
	private static final int DEFAULT_CLIENT_SSL_PORT = 443;
	
	/** The Constant DEFAULT_SERVER_SSL_PORT. */
	private static final int DEFAULT_SERVER_SSL_PORT = 443;
	
	/** The Constant DEFAULT_NB_OF_PARSER_THREADS. */
	private static final int DEFAULT_NB_OF_PARSER_THREADS = Runtime.getRuntime().availableProcessors();

	/** The is client SSL mode. */
	private boolean isClientSSLMode;
	
	/** The is server SSL mode. */
	private boolean isServerSSLMode;
	
	/** The server SSL port. */
	private int serverSSLPort;
	
	/** The client SSL port. */
	private int clientSSLPort;

	/**
	 * Instantiates a new http configuration.
	 *
	 * @param capabilities the capabilities
	 */
	public HttpConfiguration(ProtocolCapabilities capabilities) {
		super(capabilities);
		nbParserThreads = DEFAULT_NB_OF_PARSER_THREADS;
	}

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.protocol.Configurable#getProtocolName()
	 */
	@Override
	public String getProtocolName() {
		return PROTOCOL_NAME;
	}

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.protocol.Configurable#getDefaultProtocolPort()
	 */
	@Override
	public int getDefaultProtocolPort() {
		return DEFAULT_PROTOCOL_PORT;
	}

	/**
	 * Checks if is client SSL mode.
	 *
	 * @return true, if is client SSL mode
	 */
	public boolean isClientSSLMode() {
		return isClientSSLMode || getListenPort() == getClientSSLPort();
	}

	/**
	 * Checks if is server SSL mode.
	 *
	 * @return true, if is server SSL mode
	 */
	public boolean isServerSSLMode() {
		return isServerSSLMode || getServerEndpoint().getPort() == getServerSSLPort();
	}

	/**
	 * Sets the SSL client.
	 *
	 * @param isSSLClient the new SSL client
	 */
	public void setSSLClient(boolean isSSLClient) {
		this.isClientSSLMode = isSSLClient;
	}

	/**
	 * Sets the SSL server.
	 *
	 * @param isSSLServer the new SSL server
	 */
	public void setSSLServer(boolean isSSLServer) {
		this.isServerSSLMode = isSSLServer;
	}

	/**
	 * Sets the server SSL port.
	 *
	 * @param serverSSLPort the new server SSL port
	 */
	public void setServerSSLPort(Integer serverSSLPort) {
		this.serverSSLPort = serverSSLPort;
	}

	/**
	 * Sets the client SSL port.
	 *
	 * @param clientSSLPort the new client SSL port
	 */
	public void setClientSSLPort(Integer clientSSLPort) {
		this.clientSSLPort = clientSSLPort;
	}

	/**
	 * Gets the client SSL port.
	 *
	 * @return the client SSL port
	 */
	public int getClientSSLPort() {
		return clientSSLPort != 0 ? clientSSLPort : DEFAULT_CLIENT_SSL_PORT;
	}

	/**
	 * Gets the server SSL port.
	 *
	 * @return the server SSL port
	 */
	public int getServerSSLPort() {
		return serverSSLPort != 0 ? serverSSLPort : DEFAULT_SERVER_SSL_PORT;
	}
}
