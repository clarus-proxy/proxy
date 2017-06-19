package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlBindMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class PgsqlBindMessageParser implements PgsqlMessageParser<PgsqlBindMessage> {

    @Override
    public PgsqlBindMessage parse(ByteBuf content) throws IOException {
        // Read portal
        CString portal = PgsqlUtilities.getCString(content);
        // Read prepared statement
        CString preparedStatement = PgsqlUtilities.getCString(content);
        // Read number of parameter formats
        short nbParameterFormats = content.readShort();
        // Read parameter formats
        List<Short> parameterFormats = new ArrayList<>(nbParameterFormats);
        while (nbParameterFormats > 0) {
            // Read parameter format
            parameterFormats.add(content.readShort());
            --nbParameterFormats;
        }
        // Read number of parameter values
        short nbParameterValues = content.readShort();
        // Read parameter values
        List<ByteBuf> parameterValues = new ArrayList<>(nbParameterValues);
        while (nbParameterValues > 0) {
            // Read length of parameter value
            int length = content.readInt();
            // Read parameter value
            parameterValues.add(length != -1 ? content.readSlice(length) : null);
            --nbParameterValues;
        }
        // Read number of result column formats
        short nbResultColumnFormats = content.readShort();
        // Read result column formats
        List<Short> resultColumnFormats = new ArrayList<>(nbResultColumnFormats);
        while (nbResultColumnFormats > 0) {
            // Read result column format
            resultColumnFormats.add(content.readShort());
            --nbResultColumnFormats;
        }
        return new PgsqlBindMessage(portal, preparedStatement, parameterFormats, parameterValues, resultColumnFormats);
    }

}
