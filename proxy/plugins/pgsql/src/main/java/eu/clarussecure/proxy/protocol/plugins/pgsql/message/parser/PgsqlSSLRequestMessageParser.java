package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSSLRequestMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlSSLRequestMessageParser implements PgsqlMessageParser<PgsqlSSLRequestMessage> {

    @Override
    public PgsqlSSLRequestMessage parse(ByteBuf content) throws IOException {
        // Read SSL request code
        int code = content.readInt();
        return new PgsqlSSLRequestMessage(code);
    }

}
