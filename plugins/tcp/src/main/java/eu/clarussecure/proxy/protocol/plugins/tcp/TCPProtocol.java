package eu.clarussecure.proxy.protocol.plugins.tcp;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;
import eu.clarussecure.proxy.spi.protocol.ProtocolExecutor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

public class TCPProtocol extends ProtocolExecutor {

    private final TCPCapabilities capabilities = new TCPCapabilities();
    private final TCPConfiguration configuration = new TCPConfiguration(capabilities);

    @Override
    public ProtocolCapabilities getCapabilities() {
        return capabilities;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    protected TCPServer<? extends ChannelInitializer<Channel>, ? extends ChannelInitializer<Channel>> buildServer() {
        return new TCPServer<>(getConfiguration(), ClientSidePipelineInitializer.class,
                ServerSidePipelineInitializer.class, 0);
    }

}
