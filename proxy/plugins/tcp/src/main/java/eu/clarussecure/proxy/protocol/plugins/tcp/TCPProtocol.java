package eu.clarussecure.proxy.protocol.plugins.tcp;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;
import eu.clarussecure.proxy.spi.protocol.ProtocolExecutor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

public class TCPProtocol extends ProtocolExecutor {

    private static class Helper {
        private static final TCPCapabilities CAPABILITIES = new TCPCapabilities();
        private static final TCPConfiguration CONFIGURATION = new TCPConfiguration(CAPABILITIES);
    }

    @Override
    public ProtocolCapabilities getCapabilities() {
        return Helper.CAPABILITIES;
    }

    @Override
    public Configuration getConfiguration() {
        return Helper.CONFIGURATION;
    }

    @Override
    protected TCPServer<? extends ChannelInitializer<Channel>> buildServer() {
        return new TCPServer<>(getConfiguration(), ClientSidePipelineInitializer.class);
    }

}
