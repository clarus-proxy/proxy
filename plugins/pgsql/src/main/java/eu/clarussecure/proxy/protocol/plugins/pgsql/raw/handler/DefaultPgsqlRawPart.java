package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

import io.netty.buffer.ByteBuf;

public abstract class DefaultPgsqlRawPart<T extends PgsqlRawPart> implements PgsqlRawPart {

    private final ByteBuf bytes;
    private boolean filter = true;

    /**
     * Creates a new instance with the specified chunk bytes.
     */
    public DefaultPgsqlRawPart(ByteBuf bytes) {
        if (bytes == null) {
            throw new NullPointerException("bytes");
        }
        this.bytes = bytes;
    }

    @Override
    public ByteBuf content() {
        return bytes;
    }

    @Override
    public T copy() {
        return replace(bytes.copy());
    }

    @Override
    public T duplicate() {
        return replace(bytes.duplicate().readerIndex(0));
    }

    @Override
    public T retainedDuplicate() {
        return replace(bytes.retainedDuplicate().readerIndex(0));
    }

    @Override
    public abstract T replace(ByteBuf bytes);

    @Override
    public int refCnt() {
        return bytes.refCnt();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T retain() {
        bytes.retain();
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T retain(int increment) {
        bytes.retain(increment);
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T touch() {
        bytes.touch();
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T touch(Object hint) {
        bytes.touch(hint);
        return (T) this;
    }

    @Override
    public boolean release() {
        return bytes.release();
    }

    @Override
    public boolean release(int decrement) {
        return bytes.release(decrement);
    }

    @Override
    public boolean filter() {
        return filter;
    }

    public void filter(boolean filter) {
        this.filter = filter;
    }
}
