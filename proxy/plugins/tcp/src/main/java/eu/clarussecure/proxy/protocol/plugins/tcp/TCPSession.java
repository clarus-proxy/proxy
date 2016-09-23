package eu.clarussecure.proxy.protocol.plugins.tcp;

import io.netty.channel.Channel;

public class TCPSession {

    private Channel clientSideChannel;

    public Channel getClientSideChannel() {
        return clientSideChannel;
    }

    public void setClientSideChannel(Channel clientSideChannel) {
        this.clientSideChannel = clientSideChannel;
    }

}
