package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlDescribeMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlDescribeMessageParser implements PgsqlMessageParser<PgsqlDescribeMessage> {

    @Override
    public PgsqlDescribeMessage parse(ByteBuf content) throws IOException {
        // Read code
        byte code = content.readByte();
        // Read name
        CString name = PgsqlUtilities.getCString(content);
        return new PgsqlDescribeMessage(code, name);
    }

}
