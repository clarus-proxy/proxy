package eu.clarussecure.proxy.protocol.plugins.pgsql.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;

public class CustomByteBufAllocator implements ByteBufAllocator {

    @Override
    public ByteBuf buffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf buffer(int initialCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf buffer(int initialCapacity, int maxCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf ioBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf heapBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf directBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDirectBufferPooled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
        throw new UnsupportedOperationException();
    }

}
