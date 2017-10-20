package eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;
import io.netty.buffer.ByteBuf;

public final class ClientTCPFrameForwarder extends ClientMessageForwarder<ByteBuf, TCPSession> {

    public ClientTCPFrameForwarder() {
        super(TCPSession.class);
    }
}
