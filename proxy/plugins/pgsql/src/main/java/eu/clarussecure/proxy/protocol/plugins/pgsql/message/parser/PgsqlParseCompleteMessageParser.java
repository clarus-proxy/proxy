package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlParseCompleteMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlParseCompleteMessageParser implements PgsqlMessageParser<PgsqlParseCompleteMessage> {

    @Override
    public PgsqlParseCompleteMessage parse(ByteBuf content) {
        return new PgsqlParseCompleteMessage();
    }

}
