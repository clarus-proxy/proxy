package eu.clarussecure.proxy.protocol.plugins.tcp;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.Channel;

public class TCPSession {

    private Channel clientSideChannel;
    private List<Channel> serverSideChannels;
    private AtomicInteger expectedConnections;

    public TCPSession() {
        expectedConnections = new AtomicInteger(0);
    }

    public Channel getClientSideChannel() {
        return clientSideChannel;
    }

    public void setClientSideChannel(Channel clientSideChannel) {
        this.clientSideChannel = clientSideChannel;
    }

    public List<Channel> getServerSideChannels() {
        if (serverSideChannels == null) {
            serverSideChannels = new ArrayList<>();
        }
        return serverSideChannels;
    }

    public Channel getServerSideChannel(int server) {
        if (server < 0 || server >= getServerSideChannels().size()) {
            throw new IndexOutOfBoundsException(
                    String.format("server: {}, number of server: {}", server, getServerSideChannels().size()));
        }
        return serverSideChannels.get(server);
    }

    public void setServerSideChannels(List<Channel> serverSideChannels) {
        this.serverSideChannels = serverSideChannels;
    }

    public void addServerSideChannel(Channel serverSideChannel) {
        getServerSideChannels().add(serverSideChannel);
    }

    public void removeServerSideChannel(Channel serverSideChannel) {
        getServerSideChannels().remove(serverSideChannel);
    }

    public int getExpectedConnections() {
        return expectedConnections.get();
    }

    public void setExpectedConnections(int expectedConnections) {
        this.expectedConnections.set(expectedConnections);
    }

    public int decrementAndGetExpectedConnections() {
        return expectedConnections.decrementAndGet();
    }
}
