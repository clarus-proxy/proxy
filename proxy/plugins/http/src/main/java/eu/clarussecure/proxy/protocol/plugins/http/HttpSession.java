/*
 * 
 */
package eu.clarussecure.proxy.protocol.plugins.http;

import eu.clarussecure.proxy.protocol.plugins.http.message.ssl.HttpSessionInitializer;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;

// TODO: Auto-generated Javadoc
/**
 * The Class HttpSession.
 */
public class HttpSession extends TCPSession {

	/** The session initializer. */
	private HttpSessionInitializer sessionInitializer;
	
	/**
	 * Gets the session initializer.
	 *
	 * @return the session initializer
	 */
	public HttpSessionInitializer getSessionInitializer() {
		if (sessionInitializer == null) {
			sessionInitializer = new HttpSessionInitializer();
		}
		return sessionInitializer;
	}
	
}
