package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawPart;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class PgsqlRawPartEncoder extends MessageToByteEncoder<PgsqlRawPart> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgsqlRawPartEncoder.class);

    private String to;

    public PgsqlRawPartEncoder(boolean frontend) {
        this.to = frontend ? "(F<-)" : "(->B)";
    }

    @Override
    protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, PgsqlRawPart msg, boolean preferDirect) throws Exception {
        return msg.getBytes().readRetainedSlice(msg.getBytes().readableBytes());
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, PgsqlRawPart msg, ByteBuf out) throws Exception {
        LOGGER.trace("{} Encoding raw message part: {}...", to, msg);
        LOGGER.trace("{} {} bytes encoded", to, out.readableBytes());
    }

}
