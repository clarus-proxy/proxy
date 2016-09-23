package eu.clarussecure.proxy.spi.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ByteProcessor;
import io.netty.util.internal.StringUtil;

public class SynchonizedByteBuf extends ByteBuf implements ConcurrentByteBuf {

    private final ByteBuf buf;

    public static SynchonizedByteBuf wrap(ByteBuf buf) {
        return new SynchonizedByteBuf(buf);
    }

    public SynchonizedByteBuf(ByteBuf buf) {
        if (buf == null) {
            throw new NullPointerException("buf");
        }
        this.buf = buf;
    }

    @Override
    public synchronized final boolean hasMemoryAddress() {
        return buf.hasMemoryAddress();
    }

    @Override
    public synchronized final long memoryAddress() {
        return buf.memoryAddress();
    }

    @Override
    public synchronized final int capacity() {
        return buf.capacity();
    }

    @Override
    public synchronized ByteBuf capacity(int newCapacity) {
        buf.capacity(newCapacity);
        return this;
    }

    @Override
    public synchronized final int maxCapacity() {
        return buf.maxCapacity();
    }

    @Override
    public synchronized final ByteBufAllocator alloc() {
        return buf.alloc();
    }

    @SuppressWarnings("deprecation")
    @Override
    public synchronized final ByteOrder order() {
        return buf.order();
    }

    @SuppressWarnings("deprecation")
    @Override
    public synchronized ByteBuf order(ByteOrder endianness) {
        return wrap(buf.order(endianness));
    }

    @Override
    public synchronized final ByteBuf unwrap() {
        return buf;
    }

    @Override
    public synchronized ByteBuf asReadOnly() {
        return wrap(buf.asReadOnly());
    }

    @Override
    public synchronized boolean isReadOnly() {
        return buf.isReadOnly();
    }

    @Override
    public synchronized final boolean isDirect() {
        return buf.isDirect();
    }

    @Override
    public synchronized final int readerIndex() {
        return buf.readerIndex();
    }

    @Override
    public synchronized final ByteBuf readerIndex(int readerIndex) {
        buf.readerIndex(readerIndex);
        return this;
    }

    @Override
    public synchronized final int writerIndex() {
        return buf.writerIndex();
    }

    @Override
    public synchronized final ByteBuf writerIndex(int writerIndex) {
        buf.writerIndex(writerIndex);
        return this;
    }

    @Override
    public synchronized ByteBuf setIndex(int readerIndex, int writerIndex) {
        buf.setIndex(readerIndex, writerIndex);
        return this;
    }

    @Override
    public synchronized final int readableBytes() {
        return buf.readableBytes();
    }

    @Override
    public synchronized final int writableBytes() {
        return buf.writableBytes();
    }

    @Override
    public synchronized final int maxWritableBytes() {
        return buf.maxWritableBytes();
    }

    @Override
    public synchronized final boolean isReadable() {
        return buf.isReadable();
    }

    @Override
    public synchronized final boolean isWritable() {
        return buf.isWritable();
    }

    @Override
    public synchronized final ByteBuf clear() {
        buf.clear();
        return this;
    }

    @Override
    public synchronized final ByteBuf markReaderIndex() {
        buf.markReaderIndex();
        return this;
    }

    @Override
    public synchronized final ByteBuf resetReaderIndex() {
        buf.resetReaderIndex();
        return this;
    }

    @Override
    public synchronized final ByteBuf markWriterIndex() {
        buf.markWriterIndex();
        return this;
    }

    @Override
    public synchronized final ByteBuf resetWriterIndex() {
        buf.resetWriterIndex();
        return this;
    }

    @Override
    public synchronized ByteBuf discardReadBytes() {
        buf.discardReadBytes();
        return this;
    }

    @Override
    public synchronized ByteBuf discardSomeReadBytes() {
        buf.discardSomeReadBytes();
        return this;
    }

    @Override
    public synchronized ByteBuf ensureWritable(int minWritableBytes) {
        buf.ensureWritable(minWritableBytes);
        return this;
    }

    @Override
    public synchronized int ensureWritable(int minWritableBytes, boolean force) {
        return buf.ensureWritable(minWritableBytes, force);
    }

    @Override
    public synchronized boolean getBoolean(int index) {
        return buf.getBoolean(index);
    }

    @Override
    public synchronized byte getByte(int index) {
        return buf.getByte(index);
    }

    @Override
    public synchronized short getUnsignedByte(int index) {
        return buf.getUnsignedByte(index);
    }

    @Override
    public synchronized short getShort(int index) {
        return buf.getShort(index);
    }

    @Override
    public synchronized short getShortLE(int index) {
        return buf.getShortLE(index);
    }

    @Override
    public synchronized int getUnsignedShort(int index) {
        return buf.getUnsignedShort(index);
    }

    @Override
    public synchronized int getUnsignedShortLE(int index) {
        return buf.getUnsignedShortLE(index);
    }

    @Override
    public synchronized int getMedium(int index) {
        return buf.getMedium(index);
    }

    @Override
    public synchronized int getMediumLE(int index) {
        return buf.getMediumLE(index);
    }

    @Override
    public synchronized int getUnsignedMedium(int index) {
        return buf.getUnsignedMedium(index);
    }

    @Override
    public synchronized int getUnsignedMediumLE(int index) {
        return buf.getUnsignedMediumLE(index);
    }

    @Override
    public synchronized int getInt(int index) {
        return buf.getInt(index);
    }

    @Override
    public synchronized int getIntLE(int index) {
        return buf.getIntLE(index);
    }

    @Override
    public synchronized long getUnsignedInt(int index) {
        return buf.getUnsignedInt(index);
    }

    @Override
    public synchronized long getUnsignedIntLE(int index) {
        return buf.getUnsignedIntLE(index);
    }

    @Override
    public synchronized long getLong(int index) {
        return buf.getLong(index);
    }

    @Override
    public synchronized long getLongLE(int index) {
        return buf.getLongLE(index);
    }

    @Override
    public synchronized char getChar(int index) {
        return buf.getChar(index);
    }

    @Override
    public synchronized float getFloat(int index) {
        return buf.getFloat(index);
    }

    @Override
    public synchronized double getDouble(int index) {
        return buf.getDouble(index);
    }

    @Override
    public synchronized ByteBuf getBytes(int index, ByteBuf dst) {
        buf.getBytes(index, dst);
        return this;
    }

    @Override
    public synchronized ByteBuf getBytes(int index, ByteBuf dst, int length) {
        buf.getBytes(index, dst, length);
        return this;
    }

    @Override
    public synchronized ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
        buf.getBytes(index, dst, dstIndex, length);
        return this;
    }

    @Override
    public synchronized ByteBuf getBytes(int index, byte[] dst) {
        buf.getBytes(index, dst);
        return this;
    }

    @Override
    public synchronized ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        buf.getBytes(index, dst, dstIndex, length);
        return this;
    }

    @Override
    public synchronized ByteBuf getBytes(int index, ByteBuffer dst) {
        buf.getBytes(index, dst);
        return this;
    }

    @Override
    public synchronized ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
        buf.getBytes(index, out, length);
        return this;
    }

    @Override
    public synchronized int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        return buf.getBytes(index, out, length);
    }

    @Override
    public synchronized int getBytes(int index, FileChannel out, long position, int length) throws IOException {
        return buf.getBytes(index, out, position, length);
    }

    @Override
    public synchronized CharSequence getCharSequence(int index, int length, Charset charset) {
        return buf.getCharSequence(index, length, charset);
    }

    @Override
    public synchronized ByteBuf setBoolean(int index, boolean value) {
        buf.setBoolean(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setByte(int index, int value) {
        buf.setByte(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setShort(int index, int value) {
        buf.setShort(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setShortLE(int index, int value) {
        buf.setShortLE(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setMedium(int index, int value) {
        buf.setMedium(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setMediumLE(int index, int value) {
        buf.setMediumLE(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setInt(int index, int value) {
        buf.setInt(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setIntLE(int index, int value) {
        buf.setIntLE(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setLong(int index, long value) {
        buf.setLong(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setLongLE(int index, long value) {
        buf.setLongLE(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setChar(int index, int value) {
        buf.setChar(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setFloat(int index, float value) {
        buf.setFloat(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setDouble(int index, double value) {
        buf.setDouble(index, value);
        return this;
    }

    @Override
    public synchronized ByteBuf setBytes(int index, ByteBuf src) {
        buf.setBytes(index, src);
        return this;
    }

    @Override
    public synchronized ByteBuf setBytes(int index, ByteBuf src, int length) {
        buf.setBytes(index, src, length);
        return this;
    }

    @Override
    public synchronized ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        buf.setBytes(index, src, srcIndex, length);
        return this;
    }

    @Override
    public synchronized ByteBuf setBytes(int index, byte[] src) {
        buf.setBytes(index, src);
        return this;
    }

    @Override
    public synchronized ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        buf.setBytes(index, src, srcIndex, length);
        return this;
    }

    @Override
    public synchronized ByteBuf setBytes(int index, ByteBuffer src) {
        buf.setBytes(index, src);
        return this;
    }

    @Override
    public synchronized int setBytes(int index, InputStream in, int length) throws IOException {
        return buf.setBytes(index, in, length);
    }

    @Override
    public synchronized int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        return buf.setBytes(index, in, length);
    }

    @Override
    public synchronized int setBytes(int index, FileChannel in, long position, int length) throws IOException {
        return buf.setBytes(index, in, position, length);
    }

    @Override
    public synchronized ByteBuf setZero(int index, int length) {
        buf.setZero(index, length);
        return this;
    }

    @Override
    public synchronized int setCharSequence(int index, CharSequence sequence, Charset charset) {
        return buf.setCharSequence(index, sequence, charset);
    }

    @Override
    public synchronized boolean readBoolean() {
        return buf.readBoolean();
    }

    @Override
    public synchronized byte readByte() {
        return buf.readByte();
    }

    @Override
    public synchronized short readUnsignedByte() {
        return buf.readUnsignedByte();
    }

    @Override
    public synchronized short readShort() {
        return buf.readShort();
    }

    @Override
    public synchronized short readShortLE() {
        return buf.readShortLE();
    }

    @Override
    public synchronized int readUnsignedShort() {
        return buf.readUnsignedShort();
    }

    @Override
    public synchronized int readUnsignedShortLE() {
        return buf.readUnsignedShortLE();
    }

    @Override
    public synchronized int readMedium() {
        return buf.readMedium();
    }

    @Override
    public synchronized int readMediumLE() {
        return buf.readMediumLE();
    }

    @Override
    public synchronized int readUnsignedMedium() {
        return buf.readUnsignedMedium();
    }

    @Override
    public synchronized int readUnsignedMediumLE() {
        return buf.readUnsignedMediumLE();
    }

    @Override
    public synchronized int readInt() {
        return buf.readInt();
    }

    @Override
    public synchronized int readIntLE() {
        return buf.readIntLE();
    }

    @Override
    public synchronized long readUnsignedInt() {
        return buf.readUnsignedInt();
    }

    @Override
    public synchronized long readUnsignedIntLE() {
        return buf.readUnsignedIntLE();
    }

    @Override
    public synchronized long readLong() {
        return buf.readLong();
    }

    @Override
    public synchronized long readLongLE() {
        return buf.readLongLE();
    }

    @Override
    public synchronized char readChar() {
        return buf.readChar();
    }

    @Override
    public synchronized float readFloat() {
        return buf.readFloat();
    }

    @Override
    public synchronized double readDouble() {
        return buf.readDouble();
    }

    @Override
    public synchronized ByteBuf readBytes(int length) {
        return wrap(buf.readBytes(length));
    }

    @Override
    public synchronized ByteBuf readSlice(int length) {
        return wrap(buf.readSlice(length));
    }

    @Override
    public synchronized ByteBuf readRetainedSlice(int length) {
        return wrap(buf.readRetainedSlice(length));
    }

    @Override
    public synchronized ByteBuf readBytes(ByteBuf dst) {
        buf.readBytes(dst);
        return this;
    }

    @Override
    public synchronized ByteBuf readBytes(ByteBuf dst, int length) {
        buf.readBytes(dst, length);
        return this;
    }

    @Override
    public synchronized ByteBuf readBytes(ByteBuf dst, int dstIndex, int length) {
        buf.readBytes(dst, dstIndex, length);
        return this;
    }

    @Override
    public synchronized ByteBuf readBytes(byte[] dst) {
        buf.readBytes(dst);
        return this;
    }

    @Override
    public synchronized ByteBuf readBytes(byte[] dst, int dstIndex, int length) {
        buf.readBytes(dst, dstIndex, length);
        return this;
    }

    @Override
    public synchronized ByteBuf readBytes(ByteBuffer dst) {
        buf.readBytes(dst);
        return this;
    }

    @Override
    public synchronized ByteBuf readBytes(OutputStream out, int length) throws IOException {
        buf.readBytes(out, length);
        return this;
    }

    @Override
    public synchronized int readBytes(GatheringByteChannel out, int length) throws IOException {
        return buf.readBytes(out, length);
    }

    @Override
    public synchronized int readBytes(FileChannel out, long position, int length) throws IOException {
        return buf.readBytes(out, position, length);
    }

    @Override
    public synchronized CharSequence readCharSequence(int length, Charset charset) {
        return buf.readCharSequence(length, charset);
    }

    @Override
    public synchronized ByteBuf skipBytes(int length) {
        buf.skipBytes(length);
        return this;
    }

    @Override
    public synchronized ByteBuf writeBoolean(boolean value) {
        buf.writeBoolean(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeByte(int value) {
        buf.writeByte(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeShort(int value) {
        buf.writeShort(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeShortLE(int value) {
        buf.writeShortLE(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeMedium(int value) {
        buf.writeMedium(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeMediumLE(int value) {
        buf.writeMediumLE(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeInt(int value) {
        buf.writeInt(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeIntLE(int value) {
        buf.writeIntLE(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeLong(long value) {
        buf.writeLong(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeLongLE(long value) {
        buf.writeLongLE(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeChar(int value) {
        buf.writeChar(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeFloat(float value) {
        buf.writeFloat(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeDouble(double value) {
        buf.writeDouble(value);
        return this;
    }

    @Override
    public synchronized ByteBuf writeBytes(ByteBuf src) {
        buf.writeBytes(src);
        return this;
    }

    @Override
    public synchronized ByteBuf writeBytes(ByteBuf src, int length) {
        buf.writeBytes(src, length);
        return this;
    }

    @Override
    public synchronized ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
        buf.writeBytes(src, srcIndex, length);
        return this;
    }

    @Override
    public synchronized ByteBuf writeBytes(byte[] src) {
        buf.writeBytes(src);
        return this;
    }

    @Override
    public synchronized ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
        buf.writeBytes(src, srcIndex, length);
        return this;
    }

    @Override
    public synchronized ByteBuf writeBytes(ByteBuffer src) {
        buf.writeBytes(src);
        return this;
    }

    @Override
    public synchronized int writeBytes(InputStream in, int length) throws IOException {
        return buf.writeBytes(in, length);
    }

    @Override
    public synchronized int writeBytes(ScatteringByteChannel in, int length) throws IOException {
        return buf.writeBytes(in, length);
    }

    @Override
    public synchronized int writeBytes(FileChannel in, long position, int length) throws IOException {
        return buf.writeBytes(in, position, length);
    }

    @Override
    public synchronized ByteBuf writeZero(int length) {
        buf.writeZero(length);
        return this;
    }

    @Override
    public synchronized int writeCharSequence(CharSequence sequence, Charset charset) {
        return buf.writeCharSequence(sequence, charset);
    }

    @Override
    public synchronized int indexOf(int fromIndex, int toIndex, byte value) {
        return buf.indexOf(fromIndex, toIndex, value);
    }

    @Override
    public synchronized int bytesBefore(byte value) {
        return buf.bytesBefore(value);
    }

    @Override
    public synchronized int bytesBefore(int length, byte value) {
        return buf.bytesBefore(length, value);
    }

    @Override
    public synchronized int bytesBefore(int index, int length, byte value) {
        return buf.bytesBefore(index, length, value);
    }

    @Override
    public synchronized int forEachByte(ByteProcessor processor) {
        return buf.forEachByte(processor);
    }

    @Override
    public synchronized int forEachByte(int index, int length, ByteProcessor processor) {
        return buf.forEachByte(index, length, processor);
    }

    @Override
    public synchronized int forEachByteDesc(ByteProcessor processor) {
        return buf.forEachByteDesc(processor);
    }

    @Override
    public synchronized int forEachByteDesc(int index, int length, ByteProcessor processor) {
        return buf.forEachByteDesc(index, length, processor);
    }

    @Override
    public synchronized ByteBuf copy() {
        return wrap(buf.copy());
    }

    @Override
    public synchronized ByteBuf copy(int index, int length) {
        return wrap(buf.copy(index, length));
    }

    @Override
    public synchronized ByteBuf slice() {
        return wrap(buf.slice());
    }

    @Override
    public synchronized ByteBuf retainedSlice() {
        return wrap(buf.retainedSlice());
    }

    @Override
    public synchronized ByteBuf slice(int index, int length) {
        return wrap(buf.slice(index, length));
    }

    @Override
    public synchronized ByteBuf retainedSlice(int index, int length) {
        return wrap(buf.retainedSlice(index, length));
    }

    @Override
    public synchronized ByteBuf duplicate() {
        return wrap(buf.duplicate());
    }

    @Override
    public synchronized ByteBuf retainedDuplicate() {
        return wrap(buf.retainedDuplicate());
    }

    @Override
    public synchronized int nioBufferCount() {
        return buf.nioBufferCount();
    }

    @Override
    public synchronized ByteBuffer nioBuffer() {
        return buf.nioBuffer();
    }

    @Override
    public synchronized ByteBuffer nioBuffer(int index, int length) {
        return buf.nioBuffer(index, length);
    }

    @Override
    public synchronized ByteBuffer[] nioBuffers() {
        return buf.nioBuffers();
    }

    @Override
    public synchronized ByteBuffer[] nioBuffers(int index, int length) {
        return buf.nioBuffers(index, length);
    }

    @Override
    public synchronized ByteBuffer internalNioBuffer(int index, int length) {
        return buf.internalNioBuffer(index, length);
    }

    @Override
    public synchronized boolean hasArray() {
        return buf.hasArray();
    }

    @Override
    public synchronized byte[] array() {
        return buf.array();
    }

    @Override
    public synchronized int arrayOffset() {
        return buf.arrayOffset();
    }

    @Override
    public synchronized String toString(Charset charset) {
        return buf.toString(charset);
    }

    @Override
    public synchronized String toString(int index, int length, Charset charset) {
        return buf.toString(index, length, charset);
    }

    @Override
    public synchronized int hashCode() {
        return buf.hashCode();
    }

    @Override
    public synchronized boolean equals(Object obj) {
        return buf.equals(obj);
    }

    @Override
    public synchronized int compareTo(ByteBuf buffer) {
        return buf.compareTo(buffer);
    }

    @Override
    public synchronized String toString() {
        return StringUtil.simpleClassName(this) + '(' + buf.toString() + ')';
    }

    @Override
    public synchronized ByteBuf retain(int increment) {
        buf.retain(increment);
        return this;
    }

    @Override
    public synchronized ByteBuf retain() {
        buf.retain();
        return this;
    }

    @Override
    public synchronized ByteBuf touch() {
        buf.touch();
        return this;
    }

    @Override
    public synchronized ByteBuf touch(Object hint) {
        buf.touch(hint);
        return this;
    }

    @Override
    public synchronized final boolean isReadable(int size) {
        return buf.isReadable(size);
    }

    @Override
    public synchronized final boolean isWritable(int size) {
        return buf.isWritable(size);
    }

    @Override
    public synchronized final int refCnt() {
        return buf.refCnt();
    }

    @Override
    public synchronized boolean release() {
        return buf.release();
    }

    @Override
    public synchronized boolean release(int decrement) {
        return buf.release(decrement);
    }
}
