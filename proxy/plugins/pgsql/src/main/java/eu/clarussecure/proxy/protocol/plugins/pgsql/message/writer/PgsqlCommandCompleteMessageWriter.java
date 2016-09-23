package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCommandCompleteMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

public class PgsqlCommandCompleteMessageWriter implements PgsqlMessageWriter<PgsqlCommandCompleteMessage> {

    @Override
    public int length(PgsqlCommandCompleteMessage msg) {
        // Compute total length
        return msg.getHeaderSize() + msg.getTag().clen();
    }

    @Override
    public Map<Integer, ByteBuf> offsets(PgsqlCommandCompleteMessage msg) {
        // Compute header size
        int headerSize = msg.getHeaderSize();
        // Compute buffer offsets
        Map<Integer, ByteBuf> offsets = Collections.singletonMap(headerSize, msg.getTag().getByteBuf());
        return offsets;
    }
    @Override
    public ByteBuf write(PgsqlCommandCompleteMessage msg, ByteBuf buffer) throws IOException {
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
        // Write tag
        ByteBuf value = msg.getTag().getByteBuf();
        writeBytes(buffer, value);
        return buffer;
    }

}
