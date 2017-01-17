package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlBindCompleteMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlBindCompleteMessageParser implements PgsqlMessageParser<PgsqlBindCompleteMessage> {

    @Override
    public PgsqlBindCompleteMessage parse(ByteBuf content) {
        return new PgsqlBindCompleteMessage();
    }

}
