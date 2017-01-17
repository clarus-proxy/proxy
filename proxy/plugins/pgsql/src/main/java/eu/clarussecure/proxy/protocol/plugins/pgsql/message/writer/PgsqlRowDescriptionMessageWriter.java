package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlRowDescriptionMessageWriter implements PgsqlMessageWriter<PgsqlRowDescriptionMessage> {

    @Override
    public int contentSize(PgsqlRowDescriptionMessage msg) {
        // Get content size
        int size = Short.BYTES;
        for (PgsqlRowDescriptionMessage.Field field : msg.getFields()) {
            size += field.getName().clen();
            size += Integer.BYTES;
            size += Short.BYTES;
            size += Integer.BYTES;
            size += Short.BYTES;
            size += Integer.BYTES;
            size += Short.BYTES;
        }
        return size;
    }

    @Override
    public Map<Integer, ByteBuf> offsets(PgsqlRowDescriptionMessage msg) {
        // Compute first part size
        int firstPartSize = msg.getHeaderSize();
        firstPartSize += Short.BYTES;
        // Compute buffer offsets
        Map<Integer, ByteBuf> offsets = new LinkedHashMap<>(msg.getFields().size());
        int offset = firstPartSize;
        for (PgsqlRowDescriptionMessage.Field field : msg.getFields()) {
            offsets.put(offset, field.getName().getByteBuf());
            offset += field.getName().clen();
            offset += Integer.BYTES;
            offset += Short.BYTES;
            offset += Integer.BYTES;
            offset += Short.BYTES;
            offset += Integer.BYTES;
            offset += Short.BYTES;
        }
        return offsets;
    }

    @Override
    public void writeContent(PgsqlRowDescriptionMessage msg, ByteBuf buffer) throws IOException {
        // Write number of fields
        buffer.writeShort(msg.getFields().size());
        // Write fields
        for (PgsqlRowDescriptionMessage.Field field : msg.getFields()) {
            writeBytes(buffer, field.getName().getByteBuf());
            buffer.writeInt(field.getTableOID());
            buffer.writeShort(field.getColumnNumber());
            buffer.writeInt((int) field.getTypeOID());
            buffer.writeShort(field.getTypeSize());
            buffer.writeInt(field.getTypeModifier());
            buffer.writeShort(field.getFormat());
        }
    }

}
