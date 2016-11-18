package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlParameterDescriptionMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlParameterDescriptionMessageParser implements PgsqlMessageParser<PgsqlParameterDescriptionMessage> {

    @Override
    public PgsqlParameterDescriptionMessage parse(ByteBuf content) throws IOException {
        // Read number of parameters
        short nbParameters = content.readShort();
        // Read parameter types
        List<Long> parameterTypeOIDs = new ArrayList<>(nbParameters);
        while (nbParameters > 0) {
            long parameterTypeOID = (long) content.readInt() & 0xffffffffl;
            parameterTypeOIDs.add(parameterTypeOID);
            --nbParameters;
        }
        return new PgsqlParameterDescriptionMessage(parameterTypeOIDs);
    }

}
