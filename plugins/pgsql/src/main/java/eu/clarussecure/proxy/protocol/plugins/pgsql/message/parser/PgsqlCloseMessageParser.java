package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCloseMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlCloseMessageParser implements PgsqlMessageParser<PgsqlCloseMessage> {

    @Override
    public PgsqlCloseMessage parse(ByteBuf content) throws IOException {
        // Read code
        byte code = content.readByte();
        // Read name
        CString name = PgsqlUtilities.getCString(content);
        return new PgsqlCloseMessage(code, name);
    }

}
