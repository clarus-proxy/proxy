package eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlMessage;
import eu.clarussecure.proxy.spi.buffer.ByteBufUtilities;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;

public interface PgsqlMessageWriter<T extends PgsqlMessage> {

    int length(T msg);

    default ByteBuf allocate(T msg, ByteBuf buffer) {
        // Compute total length
        int len = length(msg);
        // Compute buffer offsets
        Map<Integer, ByteBuf> offsets = offsets(msg);
        // Determinate if allocation is necessary
        boolean alloc = buffer == null || len > buffer.capacity();
        if (!alloc && buffer != null) {
            // Check if buffer positions are all ok
            alloc = offsets.entrySet().stream()
                    .anyMatch(e -> !ByteBufUtilities.containsAt(buffer, e.getValue(), e.getKey()));
        }
        ByteBuf newBuffer;
        if (alloc) {
            ByteBufAllocator allocator = buffer == null ? UnpooledByteBufAllocator.DEFAULT : buffer.alloc();
            if (offsets.isEmpty()) {
                // Allocate buffer
                newBuffer = allocator.buffer(len);
            } else {
                // Allocate intermediate buffers
                List<ByteBuf> components = new ArrayList<>(2 * offsets.size());
                int previousOffset = 0;
                for (Map.Entry<Integer, ByteBuf> entry : offsets.entrySet()) {
                    int offset = entry.getKey();
                    ByteBuf msgBuffer = entry.getValue();
                    int intermediateSize = offset - previousOffset;
                    if (intermediateSize > 0) {
                        // Allocate intermediate buffer
                        ByteBuf intermediateBuffer = allocator.buffer(intermediateSize).writerIndex(intermediateSize);
                        components.add(intermediateBuffer);
                    }
                    components.add(msgBuffer);
                    previousOffset = offset + msgBuffer.capacity();
                }
                int intermediateSize = len - previousOffset;
                if (intermediateSize > 0) {
                    // Allocate intermediate buffer
                    ByteBuf intermediateBuffer = allocator.buffer(intermediateSize).writerIndex(intermediateSize);
                    components.add(intermediateBuffer);
                }
                // Allocate composite buffer
                newBuffer = buffer.alloc().compositeBuffer(components.size()).addComponents(components);
            }
        } else /*if (buffer != null && len <= buffer.capacity()) && ByteBufUtilities.containsAt(buffer, <msg's buffers>, <msg's buffer offsets>)*/ {
            newBuffer = buffer.retainedSlice(0, len);
        }
        newBuffer.writerIndex(0);
        return newBuffer;
    }

    default Map<Integer, ByteBuf> offsets(T msg) {
        return Collections.emptyMap();
    }

    default ByteBuf writeBytes(ByteBuf dst, ByteBuf src) {
        return writeBytes(dst, src, true);
    }

    default ByteBuf writeBytes(ByteBuf dst, ByteBuf src, boolean release) {
        boolean skipCopy = false;
        if (dst instanceof CompositeByteBuf) {
            ByteBuf buffer1 = ((CompositeByteBuf)dst).internalComponentAtOffset(dst.writerIndex());
            buffer1 = ByteBufUtilities.unwrap(buffer1).unwrap();
            ByteBuf buffer2 = ByteBufUtilities.unwrap(src);
            skipCopy = buffer1 == buffer2 || buffer1 == buffer2.unwrap();
        } else {
            skipCopy = ByteBufUtilities.containsAt(dst, src, dst.writerIndex());
        }
        if (skipCopy) {
            dst.writerIndex(dst.writerIndex() + src.readableBytes());
        } else {
            dst.writeBytes(src);
            if (release) {
                src.release();
            }
        }
        return dst;
    }

    ByteBuf write(T msg, ByteBuf buffer) throws IOException;
}
