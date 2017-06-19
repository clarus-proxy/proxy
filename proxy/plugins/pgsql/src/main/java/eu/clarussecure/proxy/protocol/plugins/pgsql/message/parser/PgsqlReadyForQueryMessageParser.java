package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlReadyForQueryMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlReadyForQueryMessageParser implements PgsqlMessageParser<PgsqlReadyForQueryMessage> {

    @Override
    public PgsqlReadyForQueryMessage parse(ByteBuf content) throws IOException {
        // Read transaction status
        byte transactionStatus = content.readByte();
        return new PgsqlReadyForQueryMessage(transactionStatus);
    }

}
