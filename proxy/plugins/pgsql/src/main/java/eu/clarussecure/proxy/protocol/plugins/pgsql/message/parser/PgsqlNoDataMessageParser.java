package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlNoDataMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlNoDataMessageParser implements PgsqlMessageParser<PgsqlNoDataMessage> {

    @Override
    public PgsqlNoDataMessage parse(ByteBuf content) {
        return new PgsqlNoDataMessage();
    }

}
