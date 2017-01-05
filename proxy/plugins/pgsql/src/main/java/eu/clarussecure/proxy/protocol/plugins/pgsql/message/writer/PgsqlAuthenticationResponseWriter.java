package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.AuthenticationHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlAuthenticationResponse;
import io.netty.buffer.ByteBuf;

public class PgsqlAuthenticationResponseWriter implements PgsqlMessageWriter<PgsqlAuthenticationResponse>{

    private static final Logger LOGGER = LoggerFactory.getLogger(PgsqlAuthenticationResponseWriter.class);
    
    @Override
    public int contentSize(PgsqlAuthenticationResponse msg) {
        // Get content size
        int size = Integer.BYTES;
        
        // Get specific field size if exist.
        if (msg.getAuthenticationParameters() != null) {
            size += msg.getAuthenticationParameters().readableBytes();
        }
        return size;
    }

    @Override
    public Map<Integer, ByteBuf> offsets(PgsqlAuthenticationResponse msg) {
        // Compute header size
        int headerSize = msg.getHeaderSize();
        // Compute buffer offsets
        int offset = headerSize;
        offset += Integer.BYTES;
        
        Map<Integer, ByteBuf> offsets = null;        
        if (msg.getAuthenticationParameters() != null) {
             offsets = Collections.singletonMap(offset, msg.getAuthenticationParameters());
        } else {
            offsets = Collections.emptyMap();
        }
        return offsets;
    }

    @Override
    public void writeContent(PgsqlAuthenticationResponse msg, ByteBuf buffer) throws IOException {
        // Write authentication type;
        LOGGER.debug("PgsqlAuthenticationResponseWriter authenticationtype: {}", msg.getAuthenticationType());
        buffer.writeInt(msg.getAuthenticationType());
        
        // Write specific bytes if exist.
        if (msg.getAuthenticationParameters() != null) {
            buffer.writeBytes(msg.getAuthenticationParameters());
        }
    }

    
}
