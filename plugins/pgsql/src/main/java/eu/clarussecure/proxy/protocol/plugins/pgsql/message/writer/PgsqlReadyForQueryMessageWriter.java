package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlReadyForQueryMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlReadyForQueryMessageWriter implements PgsqlMessageWriter<PgsqlReadyForQueryMessage> {

    @Override
    public int contentSize(PgsqlReadyForQueryMessage msg) {
        // Get content size
        return Byte.BYTES;
    }

    @Override
    public void writeContent(PgsqlReadyForQueryMessage msg, ByteBuf buffer) throws IOException {
        // Write transaction status
        buffer.writeByte(msg.getTransactionStatus());
    }

}
