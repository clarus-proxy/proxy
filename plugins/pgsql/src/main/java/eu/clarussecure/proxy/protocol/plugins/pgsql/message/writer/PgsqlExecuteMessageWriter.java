package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlExecuteMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlExecuteMessageWriter implements PgsqlMessageWriter<PgsqlExecuteMessage> {

    @Override
    public int contentSize(PgsqlExecuteMessage msg) {
        // Get content size
        int size = msg.getPortal().clen();
        size += Integer.BYTES;
        return size;
    }

    @Override
    public Map<Integer, ByteBuf> offsets(PgsqlExecuteMessage msg) {
        // Compute header size
        int headerSize = msg.getHeaderSize();
        // Compute buffer offsets
        int offset = headerSize;
        Map<Integer, ByteBuf> offsets = Collections.singletonMap(offset, msg.getPortal().getByteBuf());
        return offsets;
    }

    @Override
    public void writeContent(PgsqlExecuteMessage msg, ByteBuf buffer) throws IOException {
        // Write portal
        ByteBuf value = msg.getPortal().getByteBuf();
        writeBytes(buffer, value);
        // Write max rows
        buffer.writeInt(msg.getMaxRows());
    }
}
