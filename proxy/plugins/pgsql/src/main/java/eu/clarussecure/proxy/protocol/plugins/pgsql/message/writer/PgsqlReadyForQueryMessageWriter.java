package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlReadyForQueryMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

public class PgsqlReadyForQueryMessageWriter implements PgsqlMessageWriter<PgsqlReadyForQueryMessage> {

    @Override
    public int length(PgsqlReadyForQueryMessage msg) {
        // Compute total length
        return msg.getHeaderSize() + Byte.BYTES;
    }

    @Override
    public ByteBuf write(PgsqlReadyForQueryMessage msg, ByteBuf buffer) throws IOException {
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
        // Write transaction status
        buffer.writeByte(msg.getTransactionStatus());
        return buffer;
    }

}
