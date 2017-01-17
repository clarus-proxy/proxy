package eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;

public class MessageForwarder<I> extends SimpleChannelInboundHandler<I> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(MessageForwarder.class);

    protected Channel sinkChannel;

    protected String direction;

    public MessageForwarder(boolean client) {
        this.direction = client ? "(C->S)" : "(C<-S)";
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, I msg) throws Exception {
        if (sinkChannel != null && sinkChannel.isActive()) {
            LOGGER.trace("{} Forward message: {} ", direction, msg);
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
