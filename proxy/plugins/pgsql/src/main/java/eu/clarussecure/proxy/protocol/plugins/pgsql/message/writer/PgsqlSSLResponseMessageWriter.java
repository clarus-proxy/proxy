package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSSLResponseMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlSSLResponseMessageWriter implements PgsqlMessageWriter<PgsqlSSLResponseMessage> {

    @Override
    public int contentSize(PgsqlSSLResponseMessage msg) {
        // Get content size
        return Byte.BYTES;
    }

    @Override
    public void writeHeader(PgsqlSSLResponseMessage msg, int length, ByteBuf buffer) throws IOException {
        // No header
    }

    @Override
    public void writeContent(PgsqlSSLResponseMessage msg, ByteBuf buffer) throws IOException {
        // Write code
        buffer.writeByte(msg.getCode());
    }

}
