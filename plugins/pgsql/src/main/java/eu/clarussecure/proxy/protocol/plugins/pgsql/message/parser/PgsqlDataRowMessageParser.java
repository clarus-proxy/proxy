package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlDataRowMessage;
import io.netty.buffer.ByteBuf;

public class PgsqlDataRowMessageParser implements PgsqlMessageParser<PgsqlDataRowMessage> {

    @Override
    public PgsqlDataRowMessage parse(ByteBuf content) throws IOException {
        // Read number of values
        short nbValues = content.readShort();
        // Read values
        List<ByteBuf> values = new ArrayList<>(nbValues);
        while (nbValues > 0) {
            int len = content.readInt();
            ByteBuf value = null;
            if (len > -1) {
                value = content.readSlice(len);
            }
            values.add(value);
            --nbValues;
        }
        return new PgsqlDataRowMessage(values);
    }

}
