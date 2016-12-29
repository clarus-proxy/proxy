package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.StringUtil;

public class DefaultPgsqlRawMessage extends DefaultPgsqlRawPart<PgsqlRawMessage> implements PgsqlRawMessage {

    private final byte type;
    private final int length;
    private final ByteBuf content;

    /**
     * Creates a new instance with the specified chunk bytes.
     */
    public DefaultPgsqlRawMessage(ByteBuf bytes) {
        this(bytes, (byte)0, 0);
    }

    /**
     * Creates a new instance with the specified chunk bytes.
     */
    public DefaultPgsqlRawMessage(ByteBuf bytes, byte type, int length) {
        super(bytes);
        this.type = type;
        this.length = length;
        this.content = bytes.slice(getBytes().readerIndex() + getHeaderSize(), getBytes().readableBytes() - getHeaderSize());
    }

    @Override
    public byte getType() {
        return type;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public ByteBuf getContent() {
        return content;
    }

    @Override
    public DefaultPgsqlRawMessage replace(ByteBuf bytes) {
        return new DefaultPgsqlRawMessage(bytes, type, length);
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) + "(bytes: " + getBytes().capacity() + ", type: " + (char) type + ", length: " + length + ", content: " + getContent().readableBytes() + ')';
    }
}
