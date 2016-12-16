package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlAuthenticationCleartextPasswordMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlAuthenticationCleartextPasswordMessageParser implements PgsqlMessageParser<PgsqlAuthenticationCleartextPasswordMessage>{

    @Override
    public PgsqlAuthenticationCleartextPasswordMessage parse(ByteBuf content) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
}
