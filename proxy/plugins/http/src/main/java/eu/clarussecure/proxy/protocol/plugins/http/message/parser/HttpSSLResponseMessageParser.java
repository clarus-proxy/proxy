package eu.clarussecure.proxy.protocol.plugins.http.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.http.message.HttpSSLResponseMessage;
import io.netty.buffer.ByteBuf;

public class HttpSSLResponseMessageParser implements HttpMessageParser<HttpSSLResponseMessage> {

	@Override
	public HttpSSLResponseMessage parse(ByteBuf content) throws IOException {
		// Read SSL response code
		byte code = content.readByte();
		return new HttpSSLResponseMessage(code);
	}

}
