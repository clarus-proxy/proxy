package eu.clarussecure.proxy.protocol.plugins.http.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.http.message.HttpSSLRequestMessage;
import io.netty.buffer.ByteBuf;

public class HttpSSLRequestMessageParser implements HttpMessageParser<HttpSSLRequestMessage> {

	@Override
	public HttpSSLRequestMessage parse(ByteBuf content) throws IOException {
		// Read SSL request code
		int code = content.readInt();
		return new HttpSSLRequestMessage(code);
	}

}
