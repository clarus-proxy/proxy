package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlBindMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlBindMessageWriter implements PgsqlMessageWriter<PgsqlBindMessage> {

    @Override
    public int contentSize(PgsqlBindMessage msg) {
        // Get content size
        int size = msg.getPortal().clen();
        size += msg.getPreparedStatement().clen();
        size += Short.BYTES;
        size += Short.BYTES * msg.getParameterFormats().size();
        size += Short.BYTES;
        for (ByteBuf parameterValue : msg.getParameterValues()) {
            size += Integer.BYTES;
            if (parameterValue != null) {
                size += parameterValue.capacity();
            }
        }
        size += Short.BYTES;
        size += Short.BYTES * msg.getResultColumnFormats().size();
        return size;
    }

    @Override
    public Map<Integer, ByteBuf> offsets(PgsqlBindMessage msg) {
        // Compute header size
        int headerSize = msg.getHeaderSize();
        // Compute buffer offsets
        Map<Integer, ByteBuf> offsets = new LinkedHashMap<>(2 + msg.getParameterValues().size());
        int offset = headerSize;
        offsets.put(offset, msg.getPortal().getByteBuf());
        offset += msg.getPortal().clen();
        offsets.put(offset, msg.getPreparedStatement().getByteBuf());
        offset += msg.getPreparedStatement().clen();
        offset += Short.BYTES;
        offset += Short.BYTES * msg.getParameterFormats().size();
        offset += Short.BYTES;
        for (ByteBuf parameterValue : msg.getParameterValues()) {
            offset += Integer.BYTES;
            if (parameterValue != null) {
                offsets.put(offset, parameterValue);
                offset += parameterValue.capacity();
            }
        }
        return offsets;
    }

    @Override
    public void writeContent(PgsqlBindMessage msg, ByteBuf buffer) throws IOException {
        // Write portal
        ByteBuf value = msg.getPortal().getByteBuf();
        writeBytes(buffer, value);
        // Write prepared statement
        value = msg.getPreparedStatement().getByteBuf();
        writeBytes(buffer, value);
        // Write number of parameter formats
        buffer.writeShort(msg.getParameterFormats().size());
        // Write parameter formats
        for (Short parameterFormat : msg.getParameterFormats()) {
            buffer.writeShort(parameterFormat.shortValue());
        }
        // Write number of parameter values
        buffer.writeShort(msg.getParameterValues().size());
        // Write parameter values
        for (ByteBuf parameterValue : msg.getParameterValues()) {
            if (parameterValue == null) {
                buffer.writeInt(-1);
            } else {
                buffer.writeInt(parameterValue.capacity());
                buffer.writeBytes(parameterValue);
            }
        }
        // Write number of result column formats
        buffer.writeShort(msg.getResultColumnFormats().size());
        // Write result column formats
        for (Short resultColumnFormat : msg.getResultColumnFormats()) {
            buffer.writeShort(resultColumnFormat.shortValue());
        }
    }
}
