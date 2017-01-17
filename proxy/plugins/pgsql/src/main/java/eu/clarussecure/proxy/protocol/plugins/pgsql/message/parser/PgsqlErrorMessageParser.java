package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlErrorMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlErrorMessageParser implements PgsqlMessageParser<PgsqlErrorMessage> {

    @Override
    public PgsqlErrorMessage parse(ByteBuf content) throws IOException {
        // Read fields
        Map<Byte, CString> fields = new LinkedHashMap<>();
        byte code = content.readByte();
        while (code != 0) {
            CString value = PgsqlUtilities.getCString(content);
            fields.put(code, value);
            code = content.readByte();
        }
        return new PgsqlErrorMessage(fields);
    }

}
