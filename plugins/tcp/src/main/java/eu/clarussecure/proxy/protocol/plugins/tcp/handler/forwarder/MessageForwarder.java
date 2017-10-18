package eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;

public abstract class MessageForwarder<I> extends SimpleChannelInboundHandler<I> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(MessageForwarder.class);

    protected boolean client;
    protected String direction;

    public MessageForwarder(boolean client) {
        this.client = client;
        this.direction = client ? "(C->S)" : "(C<-S)";
    }

    protected void forwardMessage(I msg, Channel sinkChannel) {
        if (sinkChannel != null && sinkChannel.isActive()) {
            LOGGER.trace("{} Forward message: {} ", direction, msg);
            ReferenceCountUtil.retain(msg);
            sinkChannel.writeAndFlush(msg);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (closeSinkChannels(ctx)) {
            List<Channel> sinkChannels = getSinkChannels(ctx);
            if (sinkChannels != null) {
                for (Channel sinkChannel : sinkChannels) {
                    if (sinkChannel.isActive()) {
                        LOGGER.debug("{} Forward close", direction);
                        sinkChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                    }
                }
            }
        }
        super.channelInactive(ctx);
    }

    protected boolean closeSinkChannels(ChannelHandlerContext ctx) {
        return true;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        List<Channel> sinkChannels = getSinkChannels(ctx);
        if (sinkChannels != null) {
            for (Channel sinkChannel : sinkChannels) {
                if (sinkChannel.isActive()) {
                    LOGGER.error("{} Close on error", direction);
                    sinkChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                }
            }
        }
        super.exceptionCaught(ctx, cause);
    }

    protected List<Channel> getSinkChannels(ChannelHandlerContext ctx) {
        List<Channel> sinkChannels = null;
        TCPSession session = ctx.channel().attr(TCPConstants.SESSION_KEY).get();
        if (session != null) {
            if (client) {
                sinkChannels = session.getServerSideChannels();
            } else {
                sinkChannels = Collections.singletonList(session.getClientSideChannel());
            }
        }
        return sinkChannels;
    }
}
