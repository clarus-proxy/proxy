package eu.clarussecure.proxy.protocol.plugins.tcp;

import io.netty.channel.Channel;

public class TCPSession {

    private Channel clientSideChannel;
    private Channel serverSideChannel;

    public Channel getClientSideChannel() {
        return clientSideChannel;
    }

    public void setClientSideChannel(Channel clientSideChannel) {
        this.clientSideChannel = clientSideChannel;
    }

    public Channel getServerSideChannel() {
        return serverSideChannel;
    }

    public void setServerSideChannel(Channel serverSideChannel) {
        this.serverSideChannel = serverSideChannel;
    }
}
