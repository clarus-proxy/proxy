package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSyncMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlSyncMessageParser implements PgsqlMessageParser<PgsqlSyncMessage> {

    @Override
    public PgsqlSyncMessage parse(ByteBuf content) {
        return new PgsqlSyncMessage();
    }

}
