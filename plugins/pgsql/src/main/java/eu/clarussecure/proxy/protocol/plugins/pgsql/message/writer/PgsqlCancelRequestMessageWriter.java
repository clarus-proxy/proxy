package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCancelRequestMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlCancelRequestMessageWriter implements PgsqlMessageWriter<PgsqlCancelRequestMessage> {

    @Override
    public int contentSize(PgsqlCancelRequestMessage msg) {
        // Get content size
        return PgsqlCancelRequestMessage.LENGTH - PgsqlCancelRequestMessage.HEADER_SIZE;
    }

    @Override
    public void writeHeader(PgsqlCancelRequestMessage msg, int length, ByteBuf buffer) throws IOException {
        // Write header (length)
        buffer.writeInt(length);
    }

    @Override
    public void writeContent(PgsqlCancelRequestMessage msg, ByteBuf buffer) throws IOException {
        // Write code
        buffer.writeInt(msg.getCode());
        // Write process ID
        buffer.writeInt(msg.getProcessId());
        // Write secret key
        buffer.writeInt(msg.getSecretKey());
    }

}
