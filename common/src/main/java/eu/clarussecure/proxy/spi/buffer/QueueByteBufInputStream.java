package eu.clarussecure.proxy.spi.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class QueueByteBufInputStream extends InputStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueByteBufInputStream.class);

    protected static final long POLL_TIMEOUT = 20000;

    /**
     * Add it to mark the end of incoming bytebuff (so end of stream).
     */
    public static final ByteBuf END_OF_STREAMS = Unpooled.EMPTY_BUFFER;

    private BlockingQueue<ByteBuf> streams;
    private ByteBuf currentBuffer;

    public QueueByteBufInputStream(ByteBuf initialBuffer) {
        super();
        this.currentBuffer = initialBuffer;
        this.currentBuffer.retain();
        this.streams = new LinkedBlockingQueue<>();
    }

    /**
     * When the internal queue is empty, the read method block until new
     * bytebuff is added to the queue. The blocking timeout is
     * {@link #POLL_TIMEOUT}.
     * 
     */
    @Override
    public int read() throws IOException {
        if (currentBuffer == null || END_OF_STREAMS == currentBuffer) {
            return -1;
        }
        int read = readByte();
        if (read < 0) {
            try {
                currentBuffer.release();
                currentBuffer = streams.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
                read = read();
            } catch (InterruptedException e) {
                // Nothing we can do here
            }
        }
        return read;
    }

    private int readByte() throws IOException {
        if (!currentBuffer.isReadable()) {
            return -1;
        }
        return currentBuffer.readByte() & 0xff;
    }

    /**
     * Add a new buffer to the queue internal queue
     * 
     * @param buffer
     */
    public void addBuffer(ByteBuf buffer) {
        LOGGER.trace("Add a new buffer to content queue");
        buffer.retain();
        streams.offer(buffer);
    }

    @Override
    public void close() throws IOException {
        if (currentBuffer != null) {
            currentBuffer.release();
        }
        streams.forEach(stream -> {
            stream.release();
        });
        super.close();
    }

    @Override
    public int available() throws IOException {
        return currentBuffer.readableBytes() - currentBuffer.readerIndex();
    }

}
