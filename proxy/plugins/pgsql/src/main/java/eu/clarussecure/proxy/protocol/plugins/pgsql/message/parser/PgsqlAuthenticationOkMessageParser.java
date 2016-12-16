package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlAuthenticationOkMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlAuthenticationOkMessageParser implements PgsqlMessageParser<PgsqlAuthenticationOkMessage>{

    @Override
    public PgsqlAuthenticationOkMessage parse(ByteBuf content) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

}
