package eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder;

import eu.clarussecure.proxy.protocol.plugins.tcp.ServerSidePipelineInitializer;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;
import io.netty.buffer.ByteBuf;

public final class ClientTCPFrameForwarder
        extends ClientMessageForwarder<ByteBuf, ServerSidePipelineInitializer, TCPSession> {

    public ClientTCPFrameForwarder() {
        super(ServerSidePipelineInitializer.class, TCPSession.class);
    }
}
