package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlParameterDescriptionMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlParameterDescriptionMessageWriter implements PgsqlMessageWriter<PgsqlParameterDescriptionMessage> {

    @Override
    public int contentSize(PgsqlParameterDescriptionMessage msg) {
        // Get content size
        int size = Short.BYTES;
        size += Integer.BYTES * msg.getParameterTypeOIDs().size();
        return size;
    }

    @Override
    public void writeContent(PgsqlParameterDescriptionMessage msg, ByteBuf buffer) throws IOException {
        // Write number of parameters
        buffer.writeShort(msg.getParameterTypeOIDs().size());
        // Write parameter types
        for (Long parameterTypeOID : msg.getParameterTypeOIDs()) {
            buffer.writeInt(parameterTypeOID.intValue());
        }
    }

}
