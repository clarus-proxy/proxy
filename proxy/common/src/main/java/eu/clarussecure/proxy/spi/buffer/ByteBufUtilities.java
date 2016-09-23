package eu.clarussecure.proxy.spi.buffer;

import io.netty.buffer.ByteBuf;

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
        if (buffer.hasMemoryAddress() && part.hasMemoryAddress()) {
            long bufferAddress = buffer.memoryAddress();
            int bufferCapacity = buffer.capacity();
            long partAddress = part.memoryAddress();
            int partCapacity = part.capacity();
            if ((partAddress - bufferAddress == position) && (bufferAddress + bufferCapacity) >= (partAddress + partCapacity)) {
                return true;
            }
        }
        return false;
    }

    public static ByteBuf unwrap(ByteBuf buffer) {
        while (WRAPPED_BYTEBUF_TYPE.isInstance(buffer) || WRAPPED_COMPOSITEBYTEBUF_TYPE.isInstance(buffer)) {
            buffer = buffer.unwrap();
        }
        return buffer;
    }

}
