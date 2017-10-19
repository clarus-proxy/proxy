package eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder;

import java.util.List;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPClient;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;

public abstract class ClientMessageForwarder<I, S extends TCPSession> extends MessageForwarder<I> {

    private Class<S> sessionType;

    public ClientMessageForwarder(Class<S> sessionType) {
        super(true);
        this.sessionType = sessionType;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug("{} new client connection from {}", direction, ctx.channel().remoteAddress());
        TCPClient<? extends TCPSession> client = new TCPClient<>(ctx, sessionType);
        client.call();
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof DirectedMessage) {
            boolean release = true;
            try {
                Object msg2 = ((DirectedMessage<?>) msg).getMsg();
                int to = ((DirectedMessage<?>) msg).getTo();
                if (acceptInboundMessage(msg2)) {
                    @SuppressWarnings("unchecked")
                    I imsg = (I) msg2;
                    forwardMessage(ctx, imsg, to);
                } else {
                    release = false;
                    ctx.fireChannelRead(msg);
                }
            } finally {
                if (/*autoRelease && */release) {
                    ReferenceCountUtil.release(msg);
                }
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, I msg) throws Exception {
        Integer preferredServerEndpoint = ctx.channel().attr(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY).get();
        if (preferredServerEndpoint == null) {
            throw new NullPointerException(TCPConstants.PREFERRED_SERVER_ENDPOINT_KEY.name() + " is not set");
        }
        forwardMessage(ctx, msg, preferredServerEndpoint);
    }

    @SuppressWarnings("unchecked")
    private void forwardMessage(ChannelHandlerContext ctx, I msg, int to) {
        List<Channel> sinkChannels = getSinkChannels(ctx);
        if (to < -1 || to >= sinkChannels.size()) {
            throw new IndexOutOfBoundsException(
                    String.format("index: {}, number of channel: {}", to, sinkChannels.size()));
        }
        if (to == -1) {
            for (int i = 0; i < sinkChannels.size(); i++) {
                Channel sinkChannel = sinkChannels.get(i);
                if (msg instanceof ByteBufHolder) {
                    I msg2 = msg;
                    if (i > 0) {
                        msg2 = (I) ((ByteBufHolder) msg).duplicate();
                    }
                    forwardMessage(msg2, sinkChannel);
                } else {
                    forwardMessage(msg, sinkChannel);
                }
            }
        } else {
            Channel sinkChannel = sinkChannels.get(to);
            forwardMessage(msg, sinkChannel);
        }
    }
}
