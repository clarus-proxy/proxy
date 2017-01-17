package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlAuthenticationResponse;
import io.netty.buffer.ByteBuf;

public class PgsqlAuthenticationResponseParser implements PgsqlMessageParser<PgsqlAuthenticationResponse> {

    @Override
    public PgsqlAuthenticationResponse parse(ByteBuf content) throws IOException {
        // Read authentication type.
        int authenticationType = content.readInt();
        // Read existing authentication parameters 
        ByteBuf authenticationParameters = null;
        if (content.readableBytes() > 0) {
            authenticationParameters = content.readSlice(content.readableBytes());
        }
        // Instantiate new PgsqlAuthenticationResponse
        return new PgsqlAuthenticationResponse(authenticationType, authenticationParameters);
    }

}
