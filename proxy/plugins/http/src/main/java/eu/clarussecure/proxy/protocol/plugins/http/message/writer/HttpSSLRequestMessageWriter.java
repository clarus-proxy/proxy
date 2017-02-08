package eu.clarussecure.proxy.protocol.plugins.http.message.writer;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.http.message.HttpSSLRequestMessage;
import io.netty.buffer.ByteBuf;

public class HttpSSLRequestMessageWriter implements HttpMessageWriter<HttpSSLRequestMessage> {

	@Override
	public int contentSize(HttpSSLRequestMessage msg) {
		// Get content size
		return Integer.BYTES;
	}

	@Override
	public void writeHeader(HttpSSLRequestMessage msg, int length, ByteBuf buffer) throws IOException {
		// Write header (length)
		buffer.writeInt(length);
	}

	@Override
	public void writeContent(HttpSSLRequestMessage msg, ByteBuf buffer) throws IOException {
		// Write code
		buffer.writeInt(msg.getCode());
	}

}
