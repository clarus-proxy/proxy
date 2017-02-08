package eu.clarussecure.proxy.protocol.plugins.http;

import eu.clarussecure.proxy.protocol.plugins.http.message.ssl.SessionInitializer;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;

public class HttpSession extends TCPSession {

	private SessionInitializer sessionInitializer;

	public SessionInitializer getSessionInitializer() {
		if (sessionInitializer == null) {
			sessionInitializer = new SessionInitializer();
		}
		return sessionInitializer;
	}

}
