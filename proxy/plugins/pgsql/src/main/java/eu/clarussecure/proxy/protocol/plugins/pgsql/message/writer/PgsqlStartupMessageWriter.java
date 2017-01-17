package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlStartupMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlStartupMessageWriter implements PgsqlMessageWriter<PgsqlStartupMessage> {

    @Override
    public int contentSize(PgsqlStartupMessage msg) {
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
    public Map<Integer, ByteBuf> offsets(PgsqlStartupMessage msg) {
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
    public void writeHeader(PgsqlStartupMessage msg, int length, ByteBuf buffer) throws IOException {
        // Write header (length)
        buffer.writeInt(length);
    }

    @Override
    public void writeContent(PgsqlStartupMessage msg, ByteBuf buffer) throws IOException {
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
