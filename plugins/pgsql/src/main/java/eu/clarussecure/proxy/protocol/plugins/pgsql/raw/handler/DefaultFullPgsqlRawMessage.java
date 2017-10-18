package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

import io.netty.buffer.ByteBuf;

public class DefaultFullPgsqlRawMessage extends DefaultPgsqlRawMessage implements FullPgsqlRawMessage {

    /**
     * Creates a new instance with the specified bytes.
     */
    public DefaultFullPgsqlRawMessage(ByteBuf bytes) {
        super(bytes);
    }

    /**
     * Creates a new instance with the specified bytes.
     */
    public DefaultFullPgsqlRawMessage(ByteBuf bytes, byte type, int length) {
        super(bytes, type, length);
    }

    @Override
    public DefaultFullPgsqlRawMessage replace(ByteBuf bytes) {
        return new DefaultFullPgsqlRawMessage(bytes, getType(), getLength());
    }

}
