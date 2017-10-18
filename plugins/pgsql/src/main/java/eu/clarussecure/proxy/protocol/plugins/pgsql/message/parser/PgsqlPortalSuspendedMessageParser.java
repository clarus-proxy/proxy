package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlPortalSuspendedMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlPortalSuspendedMessageParser implements PgsqlMessageParser<PgsqlPortalSuspendedMessage> {

    @Override
    public PgsqlPortalSuspendedMessage parse(ByteBuf content) {
        return new PgsqlPortalSuspendedMessage();
    }

}
