package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCancelRequestMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlCancelRequestMessageParser implements PgsqlMessageParser<PgsqlCancelRequestMessage> {

    @Override
    public PgsqlCancelRequestMessage parse(ByteBuf content) throws IOException {
        // Read cancel request code
        int code = content.readInt();
        // Read process ID
        int processId = content.readInt();
        // Read secret key
        int secretKey = content.readInt();
        return new PgsqlCancelRequestMessage(code, processId, secretKey);
    }

}
