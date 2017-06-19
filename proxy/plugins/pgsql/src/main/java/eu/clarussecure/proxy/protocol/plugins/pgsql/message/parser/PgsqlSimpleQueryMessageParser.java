package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSimpleQueryMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlSimpleQueryMessageParser implements PgsqlMessageParser<PgsqlSimpleQueryMessage> {

    @Override
    public PgsqlSimpleQueryMessage parse(ByteBuf content) throws IOException {
        // Read query
        CString query = PgsqlUtilities.getCString(content);
        return new PgsqlSimpleQueryMessage(query);
    }

}
