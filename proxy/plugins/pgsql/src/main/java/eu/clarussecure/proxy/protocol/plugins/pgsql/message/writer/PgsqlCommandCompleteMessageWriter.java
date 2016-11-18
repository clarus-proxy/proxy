package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCommandCompleteMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlCommandCompleteMessageWriter implements PgsqlMessageWriter<PgsqlCommandCompleteMessage> {

    @Override
    public int contentSize(PgsqlCommandCompleteMessage msg) {
        // Get content size
        return msg.getTag().clen();
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
    public void writeContent(PgsqlCommandCompleteMessage msg, ByteBuf buffer) throws IOException {
        // Write tag
        ByteBuf value = msg.getTag().getByteBuf();
        writeBytes(buffer, value);
    }

}
