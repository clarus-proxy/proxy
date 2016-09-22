package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.forwarder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawPart;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;

public class PgsqlRawPartForwarder extends SimpleChannelInboundHandler<PgsqlRawPart> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(PgsqlRawPartForwarder.class);

    protected Channel sinkChannel;

    protected String direction;

    public PgsqlRawPartForwarder(boolean frontend) {
        this.direction = frontend ? "(F->B)" : "(F<-B)";
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PgsqlRawPart msg) throws Exception {
        if (sinkChannel != null && sinkChannel.isActive()) {
            LOGGER.trace("{} Forward raw message part: {} ", direction, msg);
            ReferenceCountUtil.retain(msg);
            sinkChannel.writeAndFlush(msg);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (sinkChannel != null && sinkChannel.isActive()) {
            LOGGER.debug("{} Forward close", direction);
            sinkChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (sinkChannel != null && sinkChannel.isActive()) {
            LOGGER.error("{} Close on error", direction);
            sinkChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
        super.exceptionCaught(ctx, cause);
    }
}
