package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlExecuteMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlExecuteMessageParser implements PgsqlMessageParser<PgsqlExecuteMessage> {

    @Override
    public PgsqlExecuteMessage parse(ByteBuf content) throws IOException {
        // Read portal
        CString portal = PgsqlUtilities.getCString(content);
        // Read max rows
        int maxRows = content.readInt();
        return new PgsqlExecuteMessage(portal, maxRows);
    }

}
