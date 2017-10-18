package eu.clarussecure.proxy.spi.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;

public class SynchronizedCompositeByteBuf extends CompositeByteBuf implements ConcurrentByteBuf {

    public SynchronizedCompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents,
            Iterable<ByteBuf> buffers) {
        super(alloc, direct, maxNumComponents, buffers);
    }

    public SynchronizedCompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents) {
        super(alloc, direct, maxNumComponents);
    }

    public SynchronizedCompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents,
            ByteBuf... buffers) {
        super(alloc, direct, maxNumComponents, buffers);
    }

    @Override
    public synchronized CompositeByteBuf addComponent(ByteBuf buffer) {
        CompositeByteBuf bytebuf = super.addComponent(buffer);
        notifyAll();
        return bytebuf;
    }

    @Override
    public synchronized CompositeByteBuf addComponents(ByteBuf... buffers) {
        CompositeByteBuf bytebuf = super.addComponents(buffers);
        notifyAll();
        return bytebuf;
    }

    @Override
    public synchronized CompositeByteBuf addComponents(Iterable<ByteBuf> buffers) {
        CompositeByteBuf bytebuf = super.addComponents(buffers);
        notifyAll();
        return bytebuf;
    }

    @Override
    public synchronized CompositeByteBuf addComponent(int cIndex, ByteBuf buffer) {
        CompositeByteBuf bytebuf = super.addComponent(cIndex, buffer);
        notifyAll();
        return bytebuf;
    }

    @Override
    public synchronized CompositeByteBuf addComponent(boolean increaseWriterIndex, ByteBuf buffer) {
        CompositeByteBuf bytebuf = super.addComponent(increaseWriterIndex, SynchonizedByteBuf.wrap(buffer));
        notifyAll();
        return bytebuf;
    }

    @Override
    public synchronized CompositeByteBuf addComponents(boolean increaseWriterIndex, ByteBuf... buffers) {
        CompositeByteBuf bytebuf = super.addComponents(increaseWriterIndex, buffers);
        notifyAll();
        return bytebuf;
    }

    @Override
    public synchronized CompositeByteBuf addComponents(boolean increaseWriterIndex, Iterable<ByteBuf> buffers) {
        CompositeByteBuf bytebuf = super.addComponents(increaseWriterIndex, buffers);
        notifyAll();
        return bytebuf;
    }

    @Override
    public synchronized CompositeByteBuf addComponent(boolean increaseWriterIndex, int cIndex, ByteBuf buffer) {
        CompositeByteBuf bytebuf = super.addComponent(increaseWriterIndex, cIndex, buffer);
        notifyAll();
        return bytebuf;
    }

    @Override
    public synchronized CompositeByteBuf addComponents(int cIndex, ByteBuf... buffers) {
        CompositeByteBuf bytebuf = super.addComponents(cIndex, buffers);
        notifyAll();
        return bytebuf;
    }

    @Override
    public synchronized CompositeByteBuf addComponents(int cIndex, Iterable<ByteBuf> buffers) {
        CompositeByteBuf bytebuf = super.addComponents(cIndex, buffers);
        notifyAll();
        return bytebuf;
    }

    @Override
    public synchronized int capacity() {
        return super.capacity();
    }

}
