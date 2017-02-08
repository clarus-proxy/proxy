package eu.clarussecure.proxy.protocol.plugins.http.message.parser;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.http.HttpUtilities;
import eu.clarussecure.proxy.protocol.plugins.http.message.HttpStartupMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class HttpStartupMessageParser implements HttpMessageParser<HttpStartupMessage> {

	@Override
	public HttpStartupMessage parse(ByteBuf content) throws IOException {
		// Read protocol
		int protocolVersion = content.readInt();
		// Read parameters
		Map<CString, CString> parameters = new LinkedHashMap<>();
		CString parameter = HttpUtilities.getCString(content);
		if (parameter == null) {
			throw new IOException("unexpected end of message");
		}
		while (parameter.length() > 0) {
			CString value = HttpUtilities.getCString(content);
			parameters.put(parameter, value);
			parameter = HttpUtilities.getCString(content);
		}
		return new HttpStartupMessage(protocolVersion, parameters);
	}

}
