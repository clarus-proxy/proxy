package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlPasswordMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlPasswordMessageParser implements PgsqlMessageParser<PgsqlPasswordMessage> {

    @Override
    public PgsqlPasswordMessage parse(ByteBuf content) throws IOException {
        // Read password.
        CString password = PgsqlUtilities.getCString(content);
        return new PgsqlPasswordMessage(password);
    }

}
