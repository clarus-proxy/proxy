package eu.clarussecure.proxy.protocol.plugins.http.message.writer;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.http.message.HttpSSLResponseMessage;
import io.netty.buffer.ByteBuf;

public class HttpSSLResponseMessageWriter implements HttpMessageWriter<HttpSSLResponseMessage> {

	@Override
	public int contentSize(HttpSSLResponseMessage msg) {
		// Get content size
		return Byte.BYTES;
	}

	@Override
	public void writeHeader(HttpSSLResponseMessage msg, int length, ByteBuf buffer) throws IOException {
		// No header
	}

	@Override
	public void writeContent(HttpSSLResponseMessage msg, ByteBuf buffer) throws IOException {
		// Write code
		buffer.writeByte(msg.getCode());
	}

}
