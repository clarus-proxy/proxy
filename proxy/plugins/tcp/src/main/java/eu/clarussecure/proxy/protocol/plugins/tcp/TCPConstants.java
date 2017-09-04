package eu.clarussecure.proxy.protocol.plugins.tcp;

import java.util.Map;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.util.AttributeKey;

public interface TCPConstants {

    AttributeKey<Configuration> CONFIGURATION_KEY = AttributeKey.<Configuration>newInstance("CONFIGURATION_KEY");

    AttributeKey<Map<String, Object>> CUSTOM_DATA_KEY = AttributeKey
            .<Map<String, Object>>newInstance("CUSTOM_DATA_KEY");

    AttributeKey<TCPSession> SESSION_KEY = AttributeKey.<TCPSession>newInstance("SESSION_KEY");

    AttributeKey<Integer> SERVER_ENDPOINT_NUMBER_KEY = AttributeKey.<Integer>newInstance("SERVER_ENDPOINT_NUMBER_KEY");

    AttributeKey<Integer> PREFERRED_SERVER_ENDPOINT_KEY = AttributeKey
            .<Integer>newInstance("PREFERRED_SERVER_ENDPOINT_KEY");

    AttributeKey<ChannelInitializer<Channel>> SERVER_INITIALIZER_KEY = AttributeKey
            .<ChannelInitializer<Channel>>newInstance("SERVER_INITIALIZER_KEY");
}
