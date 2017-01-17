package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSimpleQueryMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlSimpleQueryMessageWriter implements PgsqlMessageWriter<PgsqlSimpleQueryMessage> {

    @Override
    public int contentSize(PgsqlSimpleQueryMessage msg) {
        // Get content size
        return msg.getQuery().clen();
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
    public void writeContent(PgsqlSimpleQueryMessage msg, ByteBuf buffer) throws IOException {
        // Write query
        ByteBuf value = msg.getQuery().getByteBuf();
        writeBytes(buffer, value);
        // Fix zero byte if necessary
        if (buffer.writableBytes() == 1) {
            buffer.writeByte(0);
        }
    }
}
