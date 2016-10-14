package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

public class PgsqlRowDescriptionMessageWriter implements PgsqlMessageWriter<PgsqlRowDescriptionMessage> {

    @Override
    public int length(PgsqlRowDescriptionMessage msg) {
        // Compute total length
        int total = msg.getHeaderSize();
        total += Short.BYTES;
        for (PgsqlRowDescriptionMessage.Field field : msg.getFields()) {
            total += field.getName().clen();
            total += Integer.BYTES;
            total += Short.BYTES;
            total += Integer.BYTES;
            total += Short.BYTES;
            total += Integer.BYTES;
            total += Short.BYTES;
        }
        return total;
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
    public ByteBuf write(PgsqlRowDescriptionMessage msg, ByteBuf buffer) throws IOException {
        // Compute total length
        int total = length(msg);
        // Allocate buffer if necessary
        if (buffer == null || buffer.writableBytes() < total) {
            ByteBufAllocator allocator = buffer == null ? UnpooledByteBufAllocator.DEFAULT : buffer.alloc();
            buffer = allocator.buffer(total);
        }
        // Write header (type + length)
        buffer.writeByte(msg.getType());
        // Compute length
        int len = total - Byte.BYTES;
        buffer.writeInt(len);
        // Write number of fields
        buffer.writeShort(msg.getFields().size());
        // Write fields
        for (PgsqlRowDescriptionMessage.Field field : msg.getFields()) {
            writeBytes(buffer, field.getName().getByteBuf());
            buffer.writeInt(field.getTableOID());
            buffer.writeShort(field.getColumnNumber());
            buffer.writeInt((int)field.getTypeOID());
            buffer.writeShort(field.getTypeSize());
            buffer.writeInt(field.getTypeModifier());
            buffer.writeShort(field.getFormat());
        }
        return buffer;
    }

}
