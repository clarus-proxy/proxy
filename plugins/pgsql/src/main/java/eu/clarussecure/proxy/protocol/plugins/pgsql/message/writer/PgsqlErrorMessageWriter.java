package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlErrorMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlErrorMessageWriter implements PgsqlMessageWriter<PgsqlErrorMessage> {

    @Override
    public int contentSize(PgsqlErrorMessage msg) {
        // Get content size
        int size = 0;
        for (Map.Entry<Byte, CString> field : msg.getFields().entrySet()) {
            size += Byte.BYTES;
            size += field.getValue().clen();
        }
        size += Byte.BYTES;
        return size;
    }

    @Override
    public Map<Integer, ByteBuf> offsets(PgsqlErrorMessage msg) {
        // Compute first part size
        int firstPartSize = msg.getHeaderSize();
        // Compute buffer offsets
        Map<Integer, ByteBuf> offsets = new LinkedHashMap<>(msg.getFields().size());
        int offset = firstPartSize;
        for (Map.Entry<Byte, CString> entry : msg.getFields().entrySet()) {
            offset += Byte.BYTES;
            offsets.put(offset, entry.getValue().getByteBuf());
            offset += entry.getValue().clen();
        }
        return offsets;
    }

    @Override
    public void writeContent(PgsqlErrorMessage msg, ByteBuf buffer) throws IOException {
        // Write fields
        for (Map.Entry<Byte, CString> field : msg.getFields().entrySet()) {
            byte key = field.getKey().byteValue();
            ByteBuf value = field.getValue().getByteBuf();
            buffer.writeByte(key);
            writeBytes(buffer, value);
        }
        buffer.writeByte(0);
    }

}
