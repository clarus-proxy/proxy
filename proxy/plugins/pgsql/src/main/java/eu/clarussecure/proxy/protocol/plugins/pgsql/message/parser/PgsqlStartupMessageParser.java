package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlStartupMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlStartupMessageParser implements PgsqlMessageParser<PgsqlStartupMessage> {

    @Override
    public PgsqlStartupMessage parse(ByteBuf content) throws IOException {
        // Read protocol
        int protocolVersion = content.readInt();
        // Read parameters
        Map<CString, CString> parameters = new LinkedHashMap<>();
        CString parameter = PgsqlUtilities.getCString(content);
        if (parameter == null) {
            throw new IOException("unexpected end of message");
        }
        while (parameter.length() > 0) {
            CString value = PgsqlUtilities.getCString(content);
            parameters.put(parameter, value);
            parameter = PgsqlUtilities.getCString(content);
        }
        return new PgsqlStartupMessage(protocolVersion, parameters);
    }

}
