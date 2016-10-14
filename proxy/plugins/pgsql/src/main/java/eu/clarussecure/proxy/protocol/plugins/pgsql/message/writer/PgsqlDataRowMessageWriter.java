package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlDataRowMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

public class PgsqlDataRowMessageWriter implements PgsqlMessageWriter<PgsqlDataRowMessage> {

    @Override
    public int length(PgsqlDataRowMessage msg) {
        // Compute total length
        int total = msg.getHeaderSize();
        total += Short.BYTES;
        for (ByteBuf value : msg.getValues()) {
            total += Integer.BYTES;
            if (value != null) {
                total += value.capacity();
            }
        }
        return total;
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
    public ByteBuf write(PgsqlDataRowMessage msg, ByteBuf buffer) throws IOException {
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
        // Write number of values
        buffer.writeShort(msg.getValues().size());
        // Write values
        for (ByteBuf value : msg.getValues()) {
            buffer.writeInt(value == null ? -1 : value.capacity());
            if (value != null) {
                writeBytes(buffer, value);
            }
        }
        return buffer;
    }

}
