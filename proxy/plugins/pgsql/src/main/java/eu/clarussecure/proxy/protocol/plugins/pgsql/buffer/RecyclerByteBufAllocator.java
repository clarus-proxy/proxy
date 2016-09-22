package eu.clarussecure.proxy.protocol.plugins.pgsql.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class RecyclerByteBufAllocator extends CustomByteBufAllocator {

    private ByteBuf recycledBuffer;
    private ChannelHandlerContext ctx;

    public RecyclerByteBufAllocator(ByteBuf recycledBuffer, ChannelHandlerContext ctx) {
        this.recycledBuffer = recycledBuffer;
        this.ctx = ctx;
    }

    @Override
    public ByteBuf buffer(int initialCapacity) {
        ByteBuf buffer;
        if (initialCapacity > recycledBuffer.capacity()) {
            // Allocate buffer
            buffer = ctx.alloc().buffer(initialCapacity);
        } else /*if (initialCapacity <= recycledBuffer.capacity())*/ {
            buffer = recycledBuffer.retainedSlice(0, initialCapacity);
            buffer.writerIndex(0);
        }
        return buffer;
    }

}
