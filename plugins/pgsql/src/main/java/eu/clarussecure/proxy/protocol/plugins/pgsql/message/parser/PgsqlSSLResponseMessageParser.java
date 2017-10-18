package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSSLResponseMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlSSLResponseMessageParser implements PgsqlMessageParser<PgsqlSSLResponseMessage> {

    @Override
    public PgsqlSSLResponseMessage parse(ByteBuf content) throws IOException {
        // Read SSL response code
        byte code = content.readByte();
        return new PgsqlSSLResponseMessage(code);
    }

}
