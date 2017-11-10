package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCloseCompleteMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlCloseCompleteMessageParser implements PgsqlMessageParser<PgsqlCloseCompleteMessage> {

    @Override
    public PgsqlCloseCompleteMessage parse(ByteBuf content) {
        return new PgsqlCloseCompleteMessage();
    }

}
