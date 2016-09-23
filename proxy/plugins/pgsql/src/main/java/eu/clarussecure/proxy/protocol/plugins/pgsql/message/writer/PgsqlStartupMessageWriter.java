package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlStartupMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

public class PgsqlStartupMessageWriter implements PgsqlMessageWriter<PgsqlStartupMessage> {

    @Override
    public int length(PgsqlStartupMessage msg) {
        // Compute total length
        int total = msg.getHeaderSize() + Integer.BYTES;
        for (Map.Entry<CString, CString> parameter : msg.getParameters().entrySet()) {
            total += parameter.getKey().clen();
            total += parameter.getValue().clen();
        }
        total += Byte.BYTES;
        return total;
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
    public ByteBuf write(PgsqlStartupMessage msg, ByteBuf buffer) throws IOException {
        // Compute total length
        int total = length(msg);
        // Allocate buffer if necessary
        if (buffer == null || buffer.writableBytes() < total) {
            ByteBufAllocator allocator = buffer == null ? UnpooledByteBufAllocator.DEFAULT : buffer.alloc();
            buffer = allocator.buffer(total);
        }
        // Write header (length)
        int len = total;
        buffer.writeInt(len);
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
        return buffer;
    }

}
