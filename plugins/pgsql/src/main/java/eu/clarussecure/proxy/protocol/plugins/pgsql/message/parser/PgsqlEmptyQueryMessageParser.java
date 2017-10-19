package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlEmptyQueryMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlEmptyQueryMessageParser implements PgsqlMessageParser<PgsqlEmptyQueryMessage> {

    @Override
    public PgsqlEmptyQueryMessage parse(ByteBuf content) {
        return new PgsqlEmptyQueryMessage();
    }

}
