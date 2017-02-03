package eu.clarussecure.proxy.protocol.plugins.http;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;

public class HttpConfiguration extends Configuration {

	public static final String PROTOCOL_NAME = "HTTP/HTTPS";

	public static final int DEFAULT_LISTEN_PORT = 8080;

	public static final int DEFAULT_NB_OF_PARSER_THREADS = Runtime.getRuntime().availableProcessors();

	public HttpConfiguration(ProtocolCapabilities capabilities) {
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
