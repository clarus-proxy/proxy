package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec;

import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.DefaultPgsqlRawMessage;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.StringUtil;

public class DefaultMutablePgsqlRawMessage extends DefaultPgsqlRawMessage implements MutablePgsqlRawMessage {

    private int missing;
    private int contentIndex;

    /**
     * Creates a new instance with the specified bytes.
     */
    public DefaultMutablePgsqlRawMessage(ByteBuf bytes) {
        super(bytes);
    }

    /**
     * Creates a new instance with the specified bytes.
     */
    public DefaultMutablePgsqlRawMessage(ByteBuf bytes, byte type, int length) {
        this(bytes, type, length, 0);
    }

    /**
     * Creates a new instance with the specified bytes.
     */
    public DefaultMutablePgsqlRawMessage(ByteBuf bytes, byte type, int length, int missing) {
        super(bytes, type, length);
        setMissing(missing);
        contentIndex = bytes.readerIndex();
    }

    @Override
    public void setMissing(int missing) {
        this.missing = missing;
    }

    @Override
    public int getMissing() {
        return missing;
    }

    @Override
    public DefaultMutablePgsqlRawMessage replace(ByteBuf bytes) {
        return new DefaultMutablePgsqlRawMessage(bytes, getType(), getLength(), missing);
    }

    @Override
    public ByteBuf getContent() {
        return getBytes().slice(contentIndex + getHeaderSize(),
                getBytes().writerIndex() - contentIndex - getHeaderSize());
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) + "(bytes: " + getBytes().capacity() + ", type: " + (char) getType()
                + ", length: " + getLength() + ", missing: " + missing + ')';
    }
}
