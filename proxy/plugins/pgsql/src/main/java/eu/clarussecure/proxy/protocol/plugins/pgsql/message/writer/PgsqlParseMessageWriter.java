package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlParseMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlParseMessageWriter implements PgsqlMessageWriter<PgsqlParseMessage> {

    @Override
    public int contentSize(PgsqlParseMessage msg) {
        // Get content size
        int size = msg.getPreparedStatement().clen();
        size += msg.getQuery().clen();
        size += Short.BYTES;
        size += Integer.BYTES * msg.getParameterTypes().size();
        return size;
    }

    @Override
    public Map<Integer, ByteBuf> offsets(PgsqlParseMessage msg) {
        // Compute header size
        int headerSize = msg.getHeaderSize();
        // Compute buffer offsets
        Map<Integer, ByteBuf> offsets = new LinkedHashMap<>(2);
        int offset = headerSize;
        offsets.put(offset, msg.getPreparedStatement().getByteBuf());
        offset += msg.getPreparedStatement().clen();
        offsets.put(offset, msg.getQuery().getByteBuf());
        return offsets;
    }

    @Override
    public void writeContent(PgsqlParseMessage msg, ByteBuf buffer) throws IOException {
        // Write prepared statement
        ByteBuf value = msg.getPreparedStatement().getByteBuf();
        writeBytes(buffer, value);
        // Write query
        value = msg.getQuery().getByteBuf();
        writeBytes(buffer, value);
        // Write number of parameter types
        buffer.writeShort(msg.getParameterTypes().size());
        // Write parameter types
        for (Long parameterType : msg.getParameterTypes()) {
            buffer.writeInt(parameterType.intValue());
        }
    }
}
