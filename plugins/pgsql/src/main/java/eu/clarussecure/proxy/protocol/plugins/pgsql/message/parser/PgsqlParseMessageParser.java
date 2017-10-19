package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlParseMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlParseMessageParser implements PgsqlMessageParser<PgsqlParseMessage> {

    @Override
    public PgsqlParseMessage parse(ByteBuf content) throws IOException {
        // Read prepared statement
        CString preparedStatement = PgsqlUtilities.getCString(content);
        // Read query
        CString query = PgsqlUtilities.getCString(content);
        // Read number of parameter types
        short nbParameterTypes = content.readShort();
        // Read parameter types
        List<Long> parameterTypes = new ArrayList<>(nbParameterTypes);
        while (nbParameterTypes > 0) {
            // Read parameter type
            long typeOID = content.readInt() & 0xffffffffl;
            parameterTypes.add(typeOID);
            --nbParameterTypes;
        }
        return new PgsqlParseMessage(preparedStatement, query, parameterTypes);
    }

}
