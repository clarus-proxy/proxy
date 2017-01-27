package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSSLRequestMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlSSLRequestMessageWriter implements PgsqlMessageWriter<PgsqlSSLRequestMessage> {

    @Override
    public int contentSize(PgsqlSSLRequestMessage msg) {
        // Get content size
        return PgsqlSSLRequestMessage.LENGTH - PgsqlSSLRequestMessage.HEADER_SIZE;
    }

    @Override
    public void writeHeader(PgsqlSSLRequestMessage msg, int length, ByteBuf buffer) throws IOException {
        // Write header (length)
        buffer.writeInt(length);
    }

    @Override
    public void writeContent(PgsqlSSLRequestMessage msg, ByteBuf buffer) throws IOException {
        // Write code
        buffer.writeInt(msg.getCode());
    }

}
