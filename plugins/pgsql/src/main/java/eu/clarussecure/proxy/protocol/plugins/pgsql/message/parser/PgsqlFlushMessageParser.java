package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlFlushMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlFlushMessageParser implements PgsqlMessageParser<PgsqlFlushMessage> {

    @Override
    public PgsqlFlushMessage parse(ByteBuf content) {
        return new PgsqlFlushMessage();
    }

}
