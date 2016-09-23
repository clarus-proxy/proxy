package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSimpleQueryMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

public class PgsqlSimpleQueryMessageWriter implements PgsqlMessageWriter<PgsqlSimpleQueryMessage> {

    @Override
    public int length(PgsqlSimpleQueryMessage msg) {
        // Compute total length
        return msg.getHeaderSize() + msg.getQuery().clen();
    }

    @Override
    public Map<Integer, ByteBuf> offsets(PgsqlSimpleQueryMessage msg) {
        // Compute header size
        int headerSize = msg.getHeaderSize();
        // Compute buffer offsets
        Map<Integer, ByteBuf> offsets = Collections.singletonMap(headerSize, msg.getQuery().getByteBuf());
        return offsets;
    }

    @Override
    public ByteBuf write(PgsqlSimpleQueryMessage msg, ByteBuf buffer) throws IOException {
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
        // Write query
        ByteBuf value = msg.getQuery().getByteBuf();
        writeBytes(buffer, value);
        // Fix zero byte if necessary
        if (buffer.writableBytes() == 1) {
            buffer.writeByte(0);
        }
        return buffer;
    }
}
