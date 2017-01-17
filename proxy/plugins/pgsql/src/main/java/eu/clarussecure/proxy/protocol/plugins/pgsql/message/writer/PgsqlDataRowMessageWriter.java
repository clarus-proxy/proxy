package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlDataRowMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlDataRowMessageWriter implements PgsqlMessageWriter<PgsqlDataRowMessage> {

    @Override
    public int contentSize(PgsqlDataRowMessage msg) {
        // Get content size
        int size = Short.BYTES;
        for (ByteBuf value : msg.getValues()) {
            size += Integer.BYTES;
            if (value != null) {
                size += value.capacity();
            }
        }
        return size;
    }

    @Override
    public Map<Integer, ByteBuf> offsets(PgsqlDataRowMessage msg) {
        // Compute first part size
        int firstPartSize = msg.getHeaderSize();
        firstPartSize += Short.BYTES;
        // Compute buffer offsets
        Map<Integer, ByteBuf> offsets = new LinkedHashMap<>(msg.getValues().size());
        int offset = firstPartSize;
        for (ByteBuf value : msg.getValues()) {
            offset += Integer.BYTES;
            if (value != null) {
                offsets.put(offset, value);
                offset += value.capacity();
            }
        }
        return offsets;
    }

    @Override
    public void writeContent(PgsqlDataRowMessage msg, ByteBuf buffer) throws IOException {
        // Write number of values
        buffer.writeShort(msg.getValues().size());
        // Write values
        for (ByteBuf value : msg.getValues()) {
            buffer.writeInt(value == null ? -1 : value.capacity());
            if (value != null) {
                writeBytes(buffer, value);
            }
        }
    }

}
