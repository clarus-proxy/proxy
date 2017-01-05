package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

import io.netty.buffer.ByteBuf;

public class DefaultLastPgsqlRawContent extends DefaultPgsqlRawContent implements LastPgsqlRawContent {

    /**
     * Creates a new instance with the specified chunk bytes.
     */
    public DefaultLastPgsqlRawContent(ByteBuf bytes) {
        super(bytes);
    }

    @Override
    public DefaultLastPgsqlRawContent replace(ByteBuf bytes) {
        return new DefaultLastPgsqlRawContent(bytes);
    }


}
