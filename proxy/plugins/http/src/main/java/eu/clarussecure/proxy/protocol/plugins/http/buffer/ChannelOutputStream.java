package eu.clarussecure.proxy.protocol.plugins.http.buffer;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;

public class ChannelOutputStream extends OutputStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelOutputStream.class);

    public static final int DEFAULT_BUFFER_SIZE = 128;

    /**
     * Mark the end of the stream
     */
    public static final ByteBuf END_BUFFER = Unpooled.EMPTY_BUFFER;

    private ByteBuf currentBuf;
    private boolean closed = false;
    private ByteBufAllocator byteBufAllocator;
    private ChannelHandlerContext out;

    public ChannelOutputStream(ChannelHandlerContext out) {
        super();
        this.out = out;
        this.byteBufAllocator = out.alloc();
        this.currentBuf = byteBufAllocator.buffer(DEFAULT_BUFFER_SIZE);
    }

    /**
     * Write byte to an internal bytebuffer until it is full. Then send it via
     * the provided channel. A new buffer is allocated. Buffer
     * size is {@link #DEFAULT_BUFFER_SIZE}
     */
    @Override
    public void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (!currentBuf.isWritable()) {
            this.sendToBackend(currentBuf);
            currentBuf = byteBufAllocator.buffer(DEFAULT_BUFFER_SIZE);
        }
        currentBuf.writeByte(b);
    }

    private void sendToBackend(ByteBuf contentBuf) {
        HttpContent httpContent = null;
        if (END_BUFFER == contentBuf) {
            httpContent = new DefaultLastHttpContent(contentBuf);
        } else {
            httpContent = new DefaultHttpContent(contentBuf);
        }
        LOGGER.trace("Send a new http content part to next handler : {}", httpContent);
        this.out.writeAndFlush(httpContent);
    }

    @Override
    public void close() throws IOException {
        if (this.currentBuf.readableBytes() > 0) {
            this.sendToBackend(this.currentBuf);
        }
        this.sendToBackend(END_BUFFER);
        closed = true;
    }

    @Override
    public void flush() {
        if (this.currentBuf.readableBytes() > 0) {
            this.sendToBackend(this.currentBuf);
            this.currentBuf = byteBufAllocator.buffer(DEFAULT_BUFFER_SIZE);
        }
    }

}
