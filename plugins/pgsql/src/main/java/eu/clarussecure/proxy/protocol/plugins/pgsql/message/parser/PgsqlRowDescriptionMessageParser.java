package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlRowDescriptionMessageParser implements PgsqlMessageParser<PgsqlRowDescriptionMessage> {

    @Override
    public PgsqlRowDescriptionMessage parse(ByteBuf content) throws IOException {
        // Read number of fields
        short nbFields = content.readShort();
        // Read fields
        List<PgsqlRowDescriptionMessage.Field> fields = new ArrayList<>(nbFields);
        while (nbFields > 0) {
            CString name = PgsqlUtilities.getCString(content);
            int tableOID = content.readInt();
            short columnNumber = content.readShort();
            long typeOID = content.readInt() & 0xffffffffl;
            short typeSize = content.readShort();
            int typeModifier = content.readInt();
            short format = content.readShort();
            PgsqlRowDescriptionMessage.Field field = new PgsqlRowDescriptionMessage.Field(name, tableOID, columnNumber,
                    typeOID, typeSize, typeModifier, format);
            fields.add(field);
            --nbFields;
        }
        return new PgsqlRowDescriptionMessage(fields);
    }

}
