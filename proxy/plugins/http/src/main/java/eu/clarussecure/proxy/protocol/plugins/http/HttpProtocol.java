/*
 * 
 */
package eu.clarussecure.proxy.protocol.plugins.http;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
	 * @param listenPort the listen port
	 */
	public HttpProtocol(int listenPort) {
		Helper.CONFIGURATION.setListenPort(listenPort);
	}

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.protocol.Protocol#getCapabilities()
	 */
	@Override
	public ProtocolCapabilities getCapabilities() {
		return Helper.CAPABILITIES;
	}

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.protocol.Protocol#getConfiguration()
	 */
	@Override
	public HttpConfiguration getConfiguration() {
		return Helper.CONFIGURATION;
	}

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.protocol.ProtocolExecutor#buildServer()
	 */
	@Override
	protected TCPServer<HttpClientPipelineInitializer> buildServer() {
		return new TCPServer<>(getConfiguration(), HttpClientPipelineInitializer.class);
	}

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.protocol.Protocol#adaptDataIds(java.lang.String[])
	 */
	@Override
	public String[] adaptDataIds(String[] dataIds) {
		// Duplicate data ids that refer to the public schema
		Pattern publicDataIdPattern = Pattern.compile("([^/]*/)(public\\.)?([^/\\.]*/[^/]*)");
		Stream<String> publicDataIds = Arrays.stream(dataIds)
				.map(id -> id.indexOf('/') == -1 ? "*/*/" + id // prepend with
																// */*/ if there
																// is no /
						: id.indexOf('/') == id.lastIndexOf('/') ? "*/" + id // prepend
																				// with
																				// */
																				// if
																				// there
																				// is
																				// one
																				// /
								: id) // do nothing if there is two /
				.map(id -> publicDataIdPattern.matcher(id)).filter(Matcher::matches)
				.map(m -> m.replaceAll(m.group(2) == null ? "$1public.$3" : "$1$3"))
				.map(id -> id.startsWith("*/*/") ? id.substring("*/*/".length()) // remove
																					// */*/
																					// if
																					// there
																					// is
						: id.startsWith("*/") ? id.substring("*/".length()) // remove
																			// */
																			// if
																			// there
																			// is
								: id); // else do nothing
		dataIds = Stream.concat(Arrays.stream(dataIds), publicDataIds).toArray(String[]::new);
		return dataIds;
	}
}
