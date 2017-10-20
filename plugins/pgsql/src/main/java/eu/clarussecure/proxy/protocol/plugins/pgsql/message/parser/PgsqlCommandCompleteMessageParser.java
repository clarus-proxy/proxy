package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCommandCompleteMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlCommandCompleteMessageParser implements PgsqlMessageParser<PgsqlCommandCompleteMessage> {

    @Override
    public PgsqlCommandCompleteMessage parse(ByteBuf content) {
        // Read tag
        CString tag = PgsqlUtilities.getCString(content);
        return new PgsqlCommandCompleteMessage(tag);
    }

}
