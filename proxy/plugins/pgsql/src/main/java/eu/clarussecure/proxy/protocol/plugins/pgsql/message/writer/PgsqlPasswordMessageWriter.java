package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlPasswordMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlPasswordMessageWriter implements PgsqlMessageWriter<PgsqlPasswordMessage> {

    @Override
    public int contentSize(PgsqlPasswordMessage msg) {
        // Get content size
        int size = msg.getPassword().clen();
        return size;
    }

    @Override
    public Map<Integer, ByteBuf> offsets(PgsqlPasswordMessage msg) {
        // Compute header size
        int headerSize = msg.getHeaderSize();
        // Compute buffer offsets
        int offset = headerSize;
        Map<Integer, ByteBuf> offsets = Collections.singletonMap(offset, msg.getPassword().getByteBuf());
        return offsets;
    }
    
    @Override
    public void writeContent(PgsqlPasswordMessage msg, ByteBuf buffer) throws IOException {
        // Write password
        ByteBuf value = msg.getPassword().getByteBuf();
        writeBytes(buffer, value);
    }
}
