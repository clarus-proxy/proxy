package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlErrorMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

public class PgsqlErrorMessageWriter implements PgsqlMessageWriter<PgsqlErrorMessage> {

    @Override
    public int length(PgsqlErrorMessage msg) {
        // Compute total length
        int total = msg.getHeaderSize();
        for (Map.Entry<Byte, CString> field : msg.getFields().entrySet()) {
            total += Byte.BYTES;
            total += field.getValue().clen();
        }
        total += Byte.BYTES;
        return total;
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
    public ByteBuf write(PgsqlErrorMessage msg, ByteBuf buffer) throws IOException {
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
        // Write fields
        for (Map.Entry<Byte, CString> field : msg.getFields().entrySet()) {
            byte key = field.getKey().byteValue();
            ByteBuf value = field.getValue().getByteBuf();
            buffer.writeByte(key);
            writeBytes(buffer, value);
        }
        buffer.writeByte(0);
        return buffer;
    }

}
