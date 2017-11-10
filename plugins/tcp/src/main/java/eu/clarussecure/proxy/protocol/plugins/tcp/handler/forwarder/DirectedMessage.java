package eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder;

import io.netty.util.ReferenceCounted;

public class DirectedMessage<I> implements ReferenceCounted {
    private final int to;
    private final I msg;

    public DirectedMessage(int to, I msg) {
        super();
        this.to = to;
        this.msg = msg;
    }

    public int getTo() {
        return to;
    }

    public I getMsg() {
        return msg;
    }

    @Override
    public int refCnt() {
        if (msg instanceof ReferenceCounted) {
            ((ReferenceCounted) msg).refCnt();
        }
        return 0;
    }

    @Override
    public ReferenceCounted retain() {
        if (msg instanceof ReferenceCounted) {
            ((ReferenceCounted) msg).retain();
        }
        return this;
    }

    @Override
    public ReferenceCounted retain(int increment) {
        if (msg instanceof ReferenceCounted) {
            ((ReferenceCounted) msg).retain(increment);
        }
        return this;
    }

    @Override
    public ReferenceCounted touch() {
        if (msg instanceof ReferenceCounted) {
            ((ReferenceCounted) msg).touch();
        }
        return this;
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        if (msg instanceof ReferenceCounted) {
            ((ReferenceCounted) msg).touch(hint);
        }
        return this;
    }

    @Override
    public boolean release() {
        if (msg instanceof ReferenceCounted) {
            return ((ReferenceCounted) msg).release();
        }
        return true;
    }

    @Override
    public boolean release(int decrement) {
        if (msg instanceof ReferenceCounted) {
            return ((ReferenceCounted) msg).release(decrement);
        }
        return true;
    }

}
