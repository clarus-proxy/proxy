package eu.clarussecure.proxy.spi.buffer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.SlicedByteBuf;

@SuppressWarnings("deprecation")
public class ByteBufUtilities {

    private static final Class<?> WRAPPED_BYTEBUF_TYPE;
    private static final Class<?> WRAPPED_COMPOSITEBYTEBUF_TYPE;
    static {
        Class<?> byteBufType = null;
        Class<?> compositeByteBufType = null;
        try {
            byteBufType = Class.forName("io.netty.buffer.WrappedByteBuf");
            compositeByteBufType = Class.forName("io.netty.buffer.WrappedCompositeByteBuf");
        } catch (ClassNotFoundException e) {
        }
        WRAPPED_BYTEBUF_TYPE = byteBufType;
        WRAPPED_COMPOSITEBYTEBUF_TYPE = compositeByteBufType;
    }

    public static boolean containsAt(ByteBuf buffer, ByteBuf part, int position) {
        if (hasMemoryAddresses(buffer) && hasMemoryAddresses(part)) {
            long[][] bufferAddresses = getMemoryAddresses(buffer);
            long[][] partAddresses = getMemoryAddresses(part);
            loop1: for (int i = 0; i < partAddresses.length; i++) {
                long partAddress = partAddresses[i][0];
                long partCapacity = partAddresses[i][1];
                for (int j = 0; j < bufferAddresses.length; j++) {
                    long bufferAddress = bufferAddresses[j][0];
                    long bufferCapacity = bufferAddresses[j][1];
                    if ((partAddress - bufferAddress == position)
                            && (bufferAddress + bufferCapacity) >= (partAddress + partCapacity)) {
                        position = 0;
                        continue loop1;
                    } else if (position >= bufferCapacity) {
                        position -= bufferCapacity;
                    }
                }
                return false;
            }
            return true;
        }
        return false;
    }

    private static boolean hasMemoryAddresses(ByteBuf buffer) {
        if (buffer.hasMemoryAddress()) {
            return true;
        }
        int offset = 0;
        int capacity = buffer.capacity();
        buffer = unwrap(buffer);
        while (buffer instanceof SlicedByteBuf) {
            SlicedByteBuf sbb = (SlicedByteBuf) buffer;
            offset += getAdjustment(sbb);
            buffer = sbb.unwrap();
        }
        if (buffer instanceof CompositeByteBuf) {
            CompositeByteBuf cbbBuffer = (CompositeByteBuf) buffer;
            do {
                int bufferIndex = cbbBuffer.toComponentIndex(offset);
                ByteBuf subBuffer = cbbBuffer.internalComponent(bufferIndex);
                if (!hasMemoryAddresses(subBuffer)) {
                    return false;
                }
                if (subBuffer.capacity() < capacity) {
                    offset += subBuffer.capacity();
                    capacity -= subBuffer.capacity();
                } else {
                    offset += capacity;
                    capacity -= capacity;
                }
            } while (capacity > 0);
            return true;
        }
        return false;
    }

    private static long[][] getMemoryAddresses(ByteBuf buffer) {
        if (buffer.hasMemoryAddress()) {
            return new long[][] { { buffer.memoryAddress(), buffer.capacity() } };
        }
        buffer = unwrap(buffer);
        int offset = 0;
        int capacity = buffer.capacity();
        while (buffer instanceof SlicedByteBuf) {
            SlicedByteBuf sbb = (SlicedByteBuf) buffer;
            offset += getAdjustment(sbb);
            buffer = sbb.unwrap();
        }
        if (buffer instanceof CompositeByteBuf) {
            CompositeByteBuf cbbBuffer = (CompositeByteBuf) buffer;
            List<long[]> memoryAddresses = new ArrayList<long[]>();
            do {
                int bufferIndex = cbbBuffer.toComponentIndex(offset);
                int bufferOffset = cbbBuffer.toByteIndex(bufferIndex);
                ByteBuf subBuffer = cbbBuffer.internalComponent(bufferIndex);
                long[][] subMemoryAddresses = getMemoryAddresses(subBuffer);
                if (subMemoryAddresses == null) {
                    return null;
                }
                for (int i = 0; i < subMemoryAddresses.length && capacity > 0; i++) {
                    long addr = subMemoryAddresses[i][0];
                    long cap = subMemoryAddresses[i][1];
                    if (i == 0) {
                        addr += offset - bufferOffset;
                        cap -= offset - bufferOffset;
                    }
                    if (cap > capacity) {
                        cap = capacity;
                    }
                    memoryAddresses.add(new long[] { addr, cap });
                    capacity -= cap;
                    offset += cap;
                }
            } while (capacity > 0);
            return memoryAddresses.stream().toArray(long[][]::new);
        }
        return null;
    }

    private static int getAdjustment(SlicedByteBuf slicedByteBuf) {
        try {
            Method adjustment = SlicedByteBuf.class.getDeclaredMethod("adjustment");
            adjustment.setAccessible(true);
            return (int) adjustment.invoke(slicedByteBuf);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException e) {
            // should not occur
        }
        return 0;
    }

    public static ByteBuf unwrap(ByteBuf buffer) {
        while (WRAPPED_BYTEBUF_TYPE.isInstance(buffer) || WRAPPED_COMPOSITEBYTEBUF_TYPE.isInstance(buffer)) {
            buffer = buffer.unwrap();
        }
        return buffer;
    }

}
