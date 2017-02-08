package eu.clarussecure.proxy.protocol.plugins.http.message.writer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.http.message.HttpStartupMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class HttpStartupMessageWriter implements HttpMessageWriter<HttpStartupMessage> {

	@Override
	public int contentSize(HttpStartupMessage msg) {
		// Get content size
		int size = Integer.BYTES;
		for (Map.Entry<CString, CString> parameter : msg.getParameters().entrySet()) {
			size += parameter.getKey().clen();
			size += parameter.getValue().clen();
		}
		size += Byte.BYTES;
		return size;
	}

	@Override
	public Map<Integer, ByteBuf> offsets(HttpStartupMessage msg) {
		// Compute first part size
		int firstPartSize = msg.getHeaderSize() + Integer.BYTES;
		// Compute buffer offsets
		Map<Integer, ByteBuf> offsets = new LinkedHashMap<>(msg.getParameters().size() * 2);
		int offset = firstPartSize;
		for (Map.Entry<CString, CString> entry : msg.getParameters().entrySet()) {
			offsets.put(offset, entry.getKey().getByteBuf());
			offset += entry.getKey().clen();
			offsets.put(offset, entry.getValue().getByteBuf());
			offset += entry.getValue().clen();
		}
		return offsets;
	}

	@Override
	public void writeHeader(HttpStartupMessage msg, int length, ByteBuf buffer) throws IOException {
		// Write header (length)
		buffer.writeInt(length);
	}

	@Override
	public void writeContent(HttpStartupMessage msg, ByteBuf buffer) throws IOException {
		// Write protocol
		buffer.writeInt(msg.getProtocolVersion());
		// Write parameters
		for (Map.Entry<CString, CString> parameter : msg.getParameters().entrySet()) {
			ByteBuf key = parameter.getKey().getByteBuf();
			writeBytes(buffer, key);
			ByteBuf value = parameter.getValue().getByteBuf();
			writeBytes(buffer, value);
		}
		buffer.writeByte(0);
	}

}
