package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlDescribeMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlDescribeMessageWriter implements PgsqlMessageWriter<PgsqlDescribeMessage> {

    @Override
    public int contentSize(PgsqlDescribeMessage msg) {
        // Get content size
        int size = Byte.BYTES;
        size += msg.getName().clen();
        return size;
    }

    @Override
    public Map<Integer, ByteBuf> offsets(PgsqlDescribeMessage msg) {
        // Compute header size
        int headerSize = msg.getHeaderSize();
        // Compute buffer offsets
        int offset = headerSize;
        offset += Byte.BYTES;
        Map<Integer, ByteBuf> offsets = Collections.singletonMap(offset, msg.getName().getByteBuf());
        return offsets;
    }

    @Override
    public void writeContent(PgsqlDescribeMessage msg, ByteBuf buffer) throws IOException {
        // Write code
        buffer.writeByte(msg.getCode());
        // Write name
        ByteBuf value = msg.getName().getByteBuf();
        writeBytes(buffer, value);
    }
}
